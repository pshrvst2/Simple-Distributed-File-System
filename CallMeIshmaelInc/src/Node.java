import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;




/**
 * @author pshrvst2
 * @Info This is the main class in the application.It has the duty to register itself, send and receive hearbeats
 * from other peers in the group.  
 *
 */
public class Node 
{
	// Naming convention, variables which begin with _ are class members.
	public static Logger _logger = Logger.getLogger(Node.class);
	public final static int _portFileListener = 2001;
	public final static int _portReceiver = 2000;
	public final static int _TCPPort = 3000;
	//public final static int _TCPPort2 = 3001;
	public static String _introducerIp = "192.17.11.153";
	public static boolean _gossipListenerThreadStop = false;
	public static boolean _fileListListenerThreadStop = false;
	public static boolean _electionListenerThreadStop = false;
	public static String _machineIp = "";
	public static String _machineId= "";
	public static int _TfailInMilliSec = 3000;
	public static int _TCleanUpInMilliSec = 3000;
	public static TimeUnit unit = MILLISECONDS;
	public static int _totalCounts= 0;
	public static int _lossCounts =0;
	public final static String _electionMessage ="ELECTION START FROM:";
	public final static String _okMessage ="ok";
	public final static String _coordinatorMessage ="NEW LEADER :";
	public final static int _timeOutForElection = 3;
	public static boolean _isIntroducer = false;
	public static int _fileMsgCounter = 0;

	
	//public static List<NodeData> _gossipList = Collections.synchronizedList(new ArrayList<NodeData>());
	// Thread safe data structure needed to store the details of all the machines in the 
	// Gossip group. Concurrent hashmap is our best option as it can store string, nodeData. 
	public static ConcurrentHashMap<String, NodeData> _gossipMap = new ConcurrentHashMap<String, NodeData>();
	
	// a new HashMap for storing the file list. This map can be accessed by different threads so it better to use concurrent hashmap
	public static ConcurrentHashMap<String, List<String>> _fileMap = new ConcurrentHashMap<String, List<String>>();

	/**
	 * @param args To ensure : Server init has to be command line.
	 */
	public static void main(String[] args)
	{
		Thread gossipListener = null;
		//Thread electionListener = null;
		try 
		{
			if(initLogging())
			{
				_logger.info("Logging is succesfully initialized! Refer log file CS425_MP2_node.log");
				System.out.println("Logging is succesfully initialized! Refer log file CS425_MP2_node.log");
			}
			else
			{
				_logger.info("Logging could not be initialized!");
				System.out.println("Logging could not be initialized!");
			}
			
			_machineIp = InetAddress.getLocalHost().getHostAddress().toString();
			
			boolean flag = true;
			while(flag)
			{
				System.out.println("\tWelcome to the CallMeIshmael Inc!");
				System.out.println("\tPress 1 to join");
				System.out.println("\tPress 2 for system info");
				System.out.println("\t!!Press any other key to shoot you!!");
				BufferedReader readerKeyboard = new BufferedReader(new InputStreamReader(System.in));
				String option = readerKeyboard.readLine();
				if(option.equalsIgnoreCase("1"))
					flag = false;
				else if (option.equalsIgnoreCase("2"))
				{
					System.out.println("\tYou are at machine: "+_machineIp);
				}
				else
				{
					System.out.println("\tYou are dead and will be on the instagram soon! RIP!!!!");
					return;
				}
			}
			
			//Concatenate the ip address with time stamp.
			Long currTimeInMiliSec = System.currentTimeMillis();
			_machineId = _machineIp + ":" + currTimeInMiliSec;
			
			_logger.info("Machine IP: "+_machineIp+" and Machine ID: "+_machineId);
			_logger.info("Adding it's entry in the Gossip list!");
			//System.out.println(machineId);
			NodeData node = new NodeData(_machineId, 1, currTimeInMiliSec, true);
			_gossipMap.put(_machineId, node);
			//_gossipList.add(node);
			
			  
			//check for introducer
			checkIntroducer(_machineIp, node);
			
			Thread fileListener = new FileListListenerThread(_portFileListener);
			fileListener.start();
			
			//Now open your socket and listen to other peers.
			gossipListener = new GossipListenerThread(_portReceiver);
			gossipListener.start();
			
			//Now open TCP socket for election
			Thread electionListener = new ElectionListenerThread(_TCPPort);
			electionListener.start();
			
			
			// logic to send periodically
			ScheduledExecutorService _schedulerService = Executors.newScheduledThreadPool(4);
			_schedulerService.scheduleAtFixedRate(new GossipSenderThread(_portReceiver), 0, 500, unit);
			
			// logic to scan the list and perform necessary actions.
			_schedulerService.scheduleAtFixedRate(new ListScanThread(), 0, 100, unit);
			
			// logic to check the leader status, if no leader exist, start an election
			_schedulerService.scheduleAtFixedRate(new LeaderScanThread(), 0, 2000, unit);
			
			
			//logic to check whether the introducer is trying to rejoin again
			if(_machineIp != _introducerIp)
			{
				// we will check this occasionally
				_schedulerService.scheduleAtFixedRate(new IntroducerRejoinThread(), 0, 5000, unit);
			}
			
			flag = true;
			while(flag)
			{
				System.out.println("\nHere are your options: ");
				System.out.println("Type 'list' to view the current membership list.");
				System.out.println("Type 'quit' to quit the group and close servers");
				System.out.println("Type 'info' to know your machine details");
				
				//new user options for MP3
				System.out.println("Type 'put <filename>' to replicate the file on SDFS");
				System.out.println("Type 'get <filename>' to to get SDFS file to local file system");
				System.out.println("Type 'delete <filename>' to delete file from SDFS");
				
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				String userCmd = reader.readLine();
				if(userCmd.equalsIgnoreCase("list"))
				{
					if(!_gossipMap.isEmpty())
					{
						String delim = "\t||\t";
						System.out.println("*********MachineId********"+delim+"**Last Seen**"+delim+"Hearbeat"+delim+"Is Active?"+delim+"PID"+delim+"Is leader?");
						_logger.info("User want to list the current members");
						_logger.info("*********MachineId********"+delim+"**Last Seen**"+delim+"Hearbeat"+delim+"Is Active?"+delim+"PID"+delim+"Is leader?");
						for (HashMap.Entry<String, NodeData> record : _gossipMap.entrySet())
						{
							NodeData temp = record.getValue();
							System.out.println(record.getKey()
									+delim+temp.getLastRecordedTime()
									+delim+temp.getHeartBeat()+"\t"
									+delim+temp.isActive()+"\t"
									+delim+temp.getPid()
									+delim+temp.isLeader());
							_logger.info(record.getKey()
									+delim+temp.getLastRecordedTime()
									+delim+temp.getHeartBeat()+"\t"
									+delim+temp.isActive()+"\t"
									+delim+temp.getPid()
									+delim+temp.isLeader());
						}
					}
				}
				else if(userCmd.startsWith("put"))
				{
					String command[] = userCmd.split("\\s");
					if(command.length != 2)
					{
						System.out.println("Enter the command correctly.");
						_logger.info("Invalid command. Enter the command correctly.");
					}
					else
					{
						// Logic - check for file at local, if exists then contact master.
						// if replicas of file already exists, then master would deny further replicas
						// else, master returns three ip addresses to which file will be replicated.
						// once file is replicated in all the three vm's, master edits the file list and gossips it all.
						
						//check for the file at local
						boolean isFilePresentAtLocal = false;
						
						if(isFilePresentAtLocal)
						{
							Thread reqInstance = new ReqSender(command[0], command[1]);
							reqInstance.start();
						}
						else
						{
							System.out.println("File not found");
						}
					}
				}
				else if(userCmd.startsWith("get"))
				{
					
				}
				else if(userCmd.startsWith("delete"))
				{
					
				}
				else if(userCmd.equalsIgnoreCase("quit"))
				{
					// send a good bye message to the Introducer so that you are quickly observed by 
					// all nodes that you are leaving.
					System.out.println("Terminating");
					_logger.info("Terminating");
					_gossipListenerThreadStop = true;
					_fileListListenerThreadStop = true;
					_electionListenerThreadStop = true;
					Node._gossipMap.get(_machineId).setIsLeader(false);
					Node._gossipMap.get(_machineId).setActive(false);
					Node._gossipMap.get(_machineId).increaseHeartBeat();
					flag = false;
					Thread.sleep(1001);
					_schedulerService.shutdownNow();					
				}
				else if(userCmd.equalsIgnoreCase("info"))
				{
					NodeData temp = _gossipMap.get(_machineId);
					String delim = "\t||\t";
					System.out.println("*********MachineId********"+delim+"**Last Seen**"+delim+"Hearbeat"+delim+"Is Active?");
					System.out.println(temp.getNodeId()
							+delim+temp.getLastRecordedTime()
							+delim+temp.getHeartBeat()+"\t"
							+delim+temp.isActive());
					_logger.info(temp.getNodeId()
							+delim+temp.getLastRecordedTime()
							+delim+temp.getHeartBeat()+""
							+delim+temp.isActive());
				}
			}
		} 
		catch (UnknownHostException e) 
		{
			_logger.error(e);
			e.printStackTrace();
		} catch (IOException e) 
		{
			_logger.error(e);
			e.printStackTrace();
		} catch (InterruptedException e) 
		{
			_logger.error(e);
			e.printStackTrace();
		}
		finally
		{
			System.out.println("Good Bye!");
			_logger.info("Good Bye!");
		}

	}

	public static boolean initLogging() 
	{
		try 
		{
			PatternLayout lyt = new PatternLayout("[%-5p] %d %c.class %t %m%n");
			RollingFileAppender rollingFileAppender = new RollingFileAppender(lyt, "CS425_MP2_node.log");
			rollingFileAppender.setLayout(lyt);
			rollingFileAppender.setName("LOGFILE");
			rollingFileAppender.setMaxFileSize("64MB");
			rollingFileAppender.activateOptions();
			Logger.getRootLogger().addAppender(rollingFileAppender);
			return true;
		} 
		catch (Exception e) 
		{
			// do nothing, just return false.
			// We don't want application to crash is logging is not working.
			return false;
		}
	}
	
	public static List<String> getLowerIdList(String id)
	{
		List<String> idList = new ArrayList<String>();
		int ownPid = _gossipMap.get(id).getPid();
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			if (record.getValue().getPid()< ownPid)
			{
				_logger.info("Found a lower Id. "+record.getKey());
				idList.add(record.getKey());
			}
		}
		
		return idList;
	}
	public static void checkIntroducer(String ip, NodeData data)
	{
		_logger.info("Checking for the introducer.");
		DatagramSocket socket = null;
		try
		{
			if(!ip.equalsIgnoreCase(_introducerIp))
			{
				//if this is the case, either the introducer is the first time initialized or trying to rejoin the existing group
				// so we try to contact all the member to contact all the member add itself to the list and retrieve the existing
				// list from any alive members
				socket = new DatagramSocket();
				int length = 0;
				byte[] buf = null;
				
				ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			    ObjectOutputStream objOpStream = new ObjectOutputStream(byteArrayOutputStream);
			    //objOpStream.writeObject(_gossipList);
			    HashMap<String, NodeData> map = new HashMap<String, NodeData>();
			    for (HashMap.Entry<String, NodeData> record : _gossipMap.entrySet())
				{
			    	map.put(record.getKey(), record.getValue());
				}
			    objOpStream.writeObject(map);
			    buf = byteArrayOutputStream.toByteArray();
			    length = buf.length;
			    
			    DatagramPacket dataPacket = new DatagramPacket(buf, length);
				dataPacket.setAddress(InetAddress.getByName(_introducerIp));
				dataPacket.setPort(_portReceiver);
				int retry = 3;
				//try three times as UDP is unreliable. At least one message will reach :)
				while(retry > 0)
				{
					socket.send(dataPacket);
					--retry;
				}
			}
			else
			{
				// If the introducer up first time or rejoin, give the highest priority 				
				data.setPId(1);
				_isIntroducer = true;
			}

		}
		catch(SocketException ex)
		{
			_logger.error(ex);
			ex.printStackTrace();
		}
		catch(IOException ioExcep)
		{
			_logger.error(ioExcep);
			ioExcep.printStackTrace();
		} 
		finally
		{
			if(socket != null)
				socket.close();
			_logger.info("Exiting from the method checkIntroducer.");
		}
	}
	
	// a simple method to pick up the leader id
	public static String getLeadId()
	{
		String leadId = null;
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			if (record.getValue().isLeader() == true)
			{
				leadId = record.getKey();
				break;
			}
		}
		return leadId; 
	}

}
