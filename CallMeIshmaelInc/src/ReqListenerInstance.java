import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author pshrvst2
 *
 */
public class ReqListenerInstance extends Thread 
{
	private static Logger log = Logger.getLogger(ReqListenerInstance.class);
	private Socket clientSocket = null;
	//private final String localFilePath = "/home/pshrvst2/local/";
	//private final String sdfsFilePath = "/home/pshrvst2/sdfs/";
	
	//private final String localFilePath = "/home/xchen135/local/";
	//private final String sdfsFilePath = "/home/xchen135/sdfs/";

	public ReqListenerInstance(Socket clientSocket) 
	{
		log.info("Connection established at socket = " + clientSocket);
		this.clientSocket = clientSocket;
	}

	public void run()
	{
		try 
		{
			String clientCommand = "";
			BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			//BufferedReader processReader = null;
			OutputStreamWriter writer = new OutputStreamWriter(clientSocket.getOutputStream());
			PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(),true);

			clientCommand = reader.readLine();
			log.info("Client fired -->" + clientCommand);
			System.out.println("Client fired -->" + clientCommand);

			// check what is the request about. Also if you are the leader, you need to
			// handle the requests well.
			String words[] = clientCommand.split(":");
			if(clientCommand.startsWith("begin"))
			{
				// its a file operation
				if(words[1].equalsIgnoreCase("put"))
				{
					File file = new File(Node.sdfsFilePath+words[2]);
					file.createNewFile();
					PrintWriter resultWriter = new PrintWriter(file);

					String line = null;
					while((line = reader.readLine()) != null)
					{
						resultWriter.println(line);
						System.out.println(line);
						if(line.startsWith("end"))
						{
							break;
						}
					}
					resultWriter.close();
				}
				else if(words[1].equalsIgnoreCase("get"))
				{
					FileReader fileReader = new FileReader(Node.sdfsFilePath+words[2]);
					BufferedReader bufReader = new BufferedReader(fileReader);
					String line = null;
					while((line = bufReader.readLine()) != null)
					{
						pw.println(line);
						System.out.println(line);
						if(line.startsWith("end"))
						{
							break;
						}
					}
					bufReader.close();
				}
				else if(words[1].equalsIgnoreCase("delete"))
				{
					Runtime rt = Runtime.getRuntime();
					String deleteCmd = "rm -rf "+Node.sdfsFilePath+words[2];
					rt.exec(new String[] { "bash", "-c", deleteCmd });
					
					pw.println("OK");
				}

			}
			else if(clientCommand.startsWith(("trans")))
			{
				// trans:sender:receiver:fileName
				String keyWords[] = clientCommand.split(":");
				String receiver = keyWords[2];
				String file = null;
				String putGlag = "";
				if(keyWords.length == 4)
				{	
					putGlag = "put";
					file = keyWords[3];
					putFile(file, receiver, putGlag);
				}
				else
				{
					// have a logic to differentiate between get and replicate
					// form of rep: [trans: ipold, ipnew, ipDelete, filename]
					// form of get: [trans: senderip: receiverip: get: filename]
					if(keyWords[3].startsWith("get"))
					{
						putGlag = "get";
						file = keyWords[4];
						putFile(file, receiver, putGlag);
					}
					else
					{
						// communicate with the sender and ask him to send the file to the receiver
						 String senderIp = keyWords[1];
						Socket newSocket = new Socket(senderIp, Node._TCPPortForRequests);
						PrintWriter pw1 = new PrintWriter(newSocket.getOutputStream(), true);
						pw.println("trans:"+senderIp+":"+receiver+":"+file);
						System.out.println("Sending commadn to sender: ");
						System.out.println("trans:"+senderIp+":"+receiver+":"+file);
						log.info("Message flushed to Sender");
						pw1.close();
						newSocket.close();
						putGlag = "rep";
					}
					
				}
				
				pw.println("OK");
				
			}
			else if(clientCommand.startsWith("end"))
			{
				// its an ACK from the client. Now update your member list.
				
				// The entire command will be in the form of 
				// end:put:foo-ip1:ip2:ip3:
				// end:delete:foo

				String entireCommand = clientCommand;
				String basicPart[] = entireCommand.split("-");
				String commandPart[] = basicPart[0].split(":");
				
				
				if(commandPart[1].equalsIgnoreCase("put"))
				{
					String temp = basicPart[1].substring(0, basicPart[1].length()-1);
					String ipPart[] = temp.split(":");
					
					Set<String> ipSet = new HashSet<String>();;
					for(String ip : ipPart)
					{
						ipSet.add(ip);
					}
					updateFileList(ipSet, commandPart[2], "put");
				}
				else if(commandPart[1].equalsIgnoreCase("delete"))
				{
					updateFileList(null, commandPart[2], "delete");
				}
			}
			else
			{
				Set<String> ipSet = null;
				// its not a file operation. Mostly this request is for leader.
				if(words[0].equalsIgnoreCase("put"))
				{
					boolean leaderAsReplica = false;
					String line = null;
					if(Node._fileMap.containsKey(words[1]))
					{
						// replica's already exist. 
						// return String "NA"
						pw.println("NA");
					}
					else
					{
						// randomly send 3 ip addresses to the request node.
						ipSet = getrandom3IpAddresses();
						if(ipSet!=null)
						{	
							for(String ip : ipSet)
							{
								if(ip.equalsIgnoreCase(Node.getLeadIp()))
									leaderAsReplica = true;

								pw.println(ip);
							}
						}
					}

					/*if(leaderAsReplica)
					{	
						File file = new File(Node.sdfsFilePath+words[1]);
						file.createNewFile();
						PrintWriter pw2 = new PrintWriter(clientSocket.getOutputStream(),true);
						PrintWriter fileWriter = new PrintWriter(file);
						while((line = reader.readLine()) != null)
						{
							//System.out.println(line);
							if(line.startsWith("end"))
							{
								// do file update and break
								if(ipSet != null)
								{
									updateFileList(ipSet, words[1], "put");
								}
								break;
							}
							fileWriter.println(line);
						}
						fileWriter.close();
					}
					else
					{
						while((line = reader.readLine()) != null)
						{
							//System.out.println(line);
							if(line.startsWith("end"))
							{
								// do file update and break
								if(ipSet != null)
								{
									updateFileList(ipSet, words[1], "put");
								}
								break;
							}
						}
					}*/
				}
				else if(words[0].equalsIgnoreCase("get"))
				{
					if(!Node._fileMap.containsKey(words[1]))
					{
						pw.println("NA");
					}
					else
					{
						List<String> ip = Node._fileMap.get(words[1]);
						String senderIp = null;
						if(!ip.get(0).equalsIgnoreCase(Node.getLeadIp()))
						{
							senderIp = ip.get(0);
							pw.println(senderIp); // later the change the logic to get(random)
						}
						else
						{
							senderIp = ip.get(1);
							pw.println(senderIp);
						}
						if(senderIp != null)
						{
							// Awesome! We found a server which has the file. Now instruct that server to put to the file to the client.
							String receiverIp = clientSocket.getInetAddress().toString().substring(1, clientSocket.getInetAddress().toString().length());
							String comnd = "trans"+":"+senderIp+":"+receiverIp+":get";
							Thread fileOprReqInstance = new ReqSender(comnd, words[1], Node.getLeadIp(), Node._TCPPortForRequests);
							fileOprReqInstance.start();
							while(fileOprReqInstance.isAlive())
							{
								// do nothing, just wait
							}
						}
					}
					pw.close();
				}
				else if(words[0].equalsIgnoreCase("delete"))
				{
					boolean isLeaderInTheList = false;
					if(!Node._fileMap.containsKey(words[1]))
					{
						pw.println("NA");
					}
					else
					{
						List<String> ip = Node._fileMap.get(words[1]);

						if(ip.contains(Node.getLeadIp()))
							isLeaderInTheList = true;

						pw.println(ip.get(0));
						pw.println(ip.get(1));
						pw.println(ip.get(2));
					}
					//pw.close();
					/*if(isLeaderInTheList)
					{
						Runtime rt = Runtime.getRuntime();
						String deleteCmd = "rm -rf "+Node.sdfsFilePath+words[1];
						Process proc = rt.exec(new String[] { "bash", "-c", deleteCmd });
						int exitValue = proc.exitValue();
						pw.println("OK");
						String line = null;
						while((line = reader.readLine()) != null)
						{
							System.out.println(line);
							if(line.startsWith("end"))
							{
								// do file update and break
								if(ipSet != null)
								{
									updateFileList(ipSet, words[1], "delete");
								}
								break;
							}
						}
					}
					else
					{
						String line = null;
						while((line = reader.readLine()) != null)
						{
							System.out.println(line);
							if(line.startsWith("end"))
							{
								// do file update and break
								if(ipSet != null)
								{
									updateFileList(ipSet, words[1], "put");
								}
								break;
							}
						}
					}*/
				}
			}

			pw.close();
			reader.close();
			writer.close();
			clientSocket.close();
			log.info("All connections closed, bye");
			System.out.println("All connections closed, bye");

		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}

	// this will return no more than 3 random ip except this machine's and the leader's
	public static Set<String> getrandom3IpAddresses() 
	{
		HashMap<String, NodeData> gossipMap = new HashMap<String, NodeData>();
		gossipMap.putAll(Node._gossipMap);
		Set<String> ips = new HashSet<String>();

		int len = gossipMap.size();
		if (len != 0) 
		{
			// retrieve the ip list from membership list
			String[] retVal = new String[len];
			int i = 0;
			for (HashMap.Entry<String, NodeData> rec : gossipMap.entrySet()) 
			{
				String id = rec.getKey();
				String[] temp = id.split(":");
				retVal[i] = temp[0];
				++i;
			}
			// get two random ip address
			// if there only one member beside this machine.
			if (len == 1) 
			{
				ips.add(retVal[0]);
			}
			// if there're two members other than itself
			else if (len == 2) 
			{
				ips.add(retVal[0]);
				ips.add(retVal[1]);
			}
			// if there're three members other than itself
			else if (len == 2) 
			{
				ips.add(retVal[0]);
				ips.add(retVal[1]);
				ips.add(retVal[2]);
			}
			// when there're more than 2 member, randomly select two
			else 
			{
				while (ips.size() < 3) 
				{
					// logic here only works for process num less than 10
					double rand = Math.random();
					rand = rand * 100;
					int index = (int) (rand % len);
					ips.add(retVal[index]);
				}
			}
		} 
		else 
		{
			// System.out.println("No member of the membership list");
		}
		return ips;
	}
	
	public void updateFileList(Set<String> ipSet, String fileName, String operation)
	{
		log.info(operation+ " on "+fileName);
		if(Node._fileMap.isEmpty())
		{
			// a new method
			String messageCounts = "msg#";
			List<String> firstList = new ArrayList<String>();
			firstList.add("0");
			firstList.add("0");
			firstList.add("0");
			Node._fileMap.put(messageCounts, firstList);
		}
		
		if(operation.equalsIgnoreCase("put"))
		{
			String counts = String.valueOf(++Node._fileMsgCounter);
			Node._fileMap.get("msg#").set(0, counts);
			List<String> addressList = new ArrayList<String>();

			for( String addr : ipSet)
			{
				addressList.add(addr);
			}
			Node._fileMap.put(fileName, addressList);
			// pass the file list to others 
			Thread fileListThread = new FileListSenderThread(Node._gossipFileListPort,true,null);
			fileListThread.start();
		}
		else if(operation.equalsIgnoreCase("delete"))
		{
			String counts = String.valueOf(++Node._fileMsgCounter);
			Node._fileMap.get("msg#").set(0, counts);
			Node._fileMap.remove(fileName);
			// pass the file list to others 
			Thread fileListThread = new FileListSenderThread(Node._gossipFileListPort,true,null);
			fileListThread.start();
		}
	}
	
	public void putFile(String fileName, String receiverIp, String putFlag)
	{
		// put file
		String fullFilePath = Node.sdfsFilePath+fileName;
		BufferedReader bufRead = null;
		try 
		{
			// logic to ping the master and get the list of ip's
			Socket socket = new Socket(receiverIp, Node._TCPPortForFileTransfers);
			//Data.O/p.Stream
			File file = new File(fullFilePath);
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			
			fileName = fileName+":"+putFlag;
			
			dos.writeUTF(fileName);
			long fileSize = file.length();
			dos.writeLong(fileSize);
			bufRead = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			int index;
			while((index=bufRead.read())!=-1)
			{
				dos.write(index);
			}
			log.info("File transfered");
			bufRead.close();
			dos.close();
			socket.close();

		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			log.error(e);
			e.printStackTrace();
		}
	}

}
