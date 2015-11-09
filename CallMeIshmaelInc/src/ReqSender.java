import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.omg.CORBA.portable.OutputStream;

/**
 * 
 */

/**
 * @author pshrvst2
 *
 */
public class ReqSender extends Thread 
{
	private static Logger log = Logger.getLogger(ReqSender.class);
	private final String userCommand;
	private final String fileName;
	private final String serverIp;
	private final int serverPort;
	//private final String localFilePath = "/home/pshrvst2/local/";
	//private final String sdfsFilePath = "/home/pshrvst2/sdfs/";
	
	//private final String localFilePath = "/home/xchen135/local/";
	//private final String sdfsFilePath = "/home/xchen135/sdfs/";
	
	public ReqSender(String cmd, String file, String serverip, int p)
	{
		this.userCommand = cmd;
		this.fileName = file;
		this.serverIp = serverip;
		this.serverPort = p;
	}

	public void run()
	{
		log.info("User command is : "+userCommand+" "+fileName);

		PrintWriter pw = null;
		BufferedReader serverReader = null;
		Socket socket;
		
		if(userCommand.equalsIgnoreCase("put"))
		{
			// get file
			try 
			{
				// logic to ping the master and get the list of ip's
				socket = new Socket(serverIp, serverPort);
				serverReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				pw = new PrintWriter(socket.getOutputStream(), true);
				pw.println(userCommand+":"+fileName);
				log.info("Message flushed to leader");
				String returnStr = null;
				long threadId = Thread.currentThread().getId();
				List<String> listOfIp = new ArrayList<String>();
				List<Thread> fileUpdateThreadList = new ArrayList<Thread>();
				boolean noResult = false;
				StringBuffer sb = new StringBuffer();
				while ((returnStr = serverReader.readLine()) != null) 
				{
					log.info(" Thread Id " + threadId + " : " + returnStr);
					if(returnStr.equalsIgnoreCase("NA"))
					{
						System.out.println("We already have replica's");
						noResult = true;
						break;
					}
					Thread fileOprReqInstance = new ReqSender("begin:"+userCommand, fileName, returnStr, Node._TCPPortForFileTransfers);
					fileOprReqInstance.start();
					fileUpdateThreadList.add(fileOprReqInstance);
					listOfIp.add(returnStr);
					sb.append(returnStr).append(":");
				}
				
				while (!fileUpdateThreadList.isEmpty()) 
				{
					for (int i = 0; i < fileUpdateThreadList.size(); i++) 
					{
						State state = fileUpdateThreadList.get(i).getState();
						if (state == Thread.State.TERMINATED
								|| state == Thread.State.BLOCKED) 
						{
							fileUpdateThreadList.remove(fileUpdateThreadList.get(i));
						}
					}
				}
				
				pw.close();
				serverReader.close();
				socket.close();
				
				if(!noResult)
				{
					Thread fileOprReqInstance = new ReqSender("end:"+userCommand+":"+fileName+"-"+sb.toString(), fileName, serverIp, serverPort);
					fileOprReqInstance.start();

					while(fileOprReqInstance.isAlive())
					{
						// do nothing and wait for it to end
					}
				}
				/*if(!listOfIp.isEmpty())
				{
					for(String ip : listOfIp)
					{
						if(!ip.equalsIgnoreCase(leaderIp))
						{
							Socket fileTransferSocket = new Socket(ip, serverPort);
							FileReader fileReader = new FileReader(fullFilePath);
							PrintWriter filePw = new PrintWriter(fileTransferSocket.getOutputStream(), true);
							filePw.println("begin:"+userCommand+":"+fileName);
							BufferedReader bufReader = new BufferedReader(fileReader);

							while((line = bufReader.readLine()) != null)
							{
								filePw.println(line);
								//System.out.println(line); 
							}
							fileReader.close();
							bufReader.close();
							filePw.close();
							fileTransferSocket.close();
						}
						else
						{
							// Leader want the replica at its sdfs
							//pw.println("begin:"+userCommand+":"+fileName);
							FileReader fileReader = new FileReader(fullFilePath);
							BufferedReader bufReader = new BufferedReader(fileReader);
							while((line = bufReader.readLine()) != null)
							{
								pw.println(line);
								//System.out.println(line); 
							}
							bufReader.close();
							fileReader.close();
						}
					}
					// send the final ack to leader that operation is done, looks to be buggy
					pw.println("end:"+userCommand+":"+fileName);
				}
				else
				{
					System.out.println("We already have replica's");
				}
				
				pw.close();
				serverReader.close();
				socket.close();*/
				
			}
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				log.error(e);
				e.printStackTrace();
			}
			
		}	
		else if(userCommand.equalsIgnoreCase("get"))
		{
			// get file from SDFS
			try 
			{
				// logic to ping the master and get one ip from which you can get the file.
				socket = new Socket(serverIp, serverPort);
				serverReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				pw = new PrintWriter(socket.getOutputStream(), true);
				pw.println(userCommand+":"+fileName);
				log.info("Message flushed to leader");
				String returnStr = null;
				long threadId = Thread.currentThread().getId();
				String remoteIp = "NF";
				while ((returnStr = serverReader.readLine()) != null) 
				{
					log.info(" Thread Id " + threadId + " : " + returnStr);
					remoteIp = returnStr;
				}
				
				if(remoteIp.equals("NF"))
				{
					System.out.println("No such file at SDFS");
				}
				//pw.println("end:"+userCommand+":"+fileName);
				pw.close();
				serverReader.close();
				socket.close();
				
			}
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				log.error(e);
				e.printStackTrace();
			}	
		}
		else if(userCommand.equalsIgnoreCase("delete"))
		{
			// get file
			String fullFilePath = Node.sdfsFilePath+fileName;
			String line = null;
			try 
			{
				// logic to ping the master and get the list of ip's
				socket = new Socket(serverIp, serverPort);
				serverReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				pw = new PrintWriter(socket.getOutputStream(), true);
				pw.println(userCommand+":"+fileName);
				log.info("Message flushed to leader");
				String returnStr = null;
				long threadId = Thread.currentThread().getId();
				List<String> listOfIp = new ArrayList<String>();
				while ((returnStr = serverReader.readLine()) != null) 
				{
					log.info(" Thread Id " + threadId + " : " + returnStr);
					if(returnStr.equalsIgnoreCase("NA"))
						break;
					listOfIp.add(returnStr);
				}
				int operationCount = 0;
				if(!listOfIp.isEmpty())
				{	
					for(String ip : listOfIp)
					{
						if(!ip.equalsIgnoreCase(serverIp))
						{
							Socket fileDeleteSocket = new Socket(ip, serverPort);
							PrintWriter filePw = new PrintWriter(fileDeleteSocket.getOutputStream(), true);
							filePw.println("begin:"+userCommand+":"+fullFilePath);
							BufferedReader bufReader = new BufferedReader(new InputStreamReader(fileDeleteSocket.getInputStream()));
							String ack = "";
							while((line = bufReader.readLine()) != null)
							{
								ack = line;
								System.out.println(line); 
							}
							if(ack.equals("OK"))
								operationCount++;

							bufReader.close();
							filePw.close();
							fileDeleteSocket.close();
						}
						else
						{
							// Leader's sdfs file to be deleted.
							//pw.println("begin:"+userCommand+":"+fullFilePath);
							String ack = "";
							while((line = serverReader.readLine()) != null)
							{
								ack = line;
								System.out.println(line); 
							}
							if(ack.equals("OK"))
								operationCount++;
						}
					}
				}
				else
				{
					System.out.println("File doesn't exist.");
				}
				
				// send the final ack to leader that operation is done
				//if(operationCount == 3)
					pw.println("end:"+userCommand+":"+fullFilePath);
				
				pw.close();
				serverReader.close();
				socket.close();
				
			}
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				log.error(e);
				e.printStackTrace();
			}
			
		}
		else if(userCommand.equalsIgnoreCase("begin:put"))
		{
			// put file
			String fullFilePath = Node.localFilePath+fileName;
			BufferedReader bufRead = null;
			try 
			{
				// logic to ping the master and get the list of ip's
				socket = new Socket(serverIp, serverPort);
				//Data.O/p.Stream
				File file = new File(fullFilePath);
				DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
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
		
		else if(userCommand.startsWith("end:put"))
		{
			// end:put command to signal the master that file operations are done and he should update the list.
			try 
			{
				// logic to ping the master and get the list of ip's
				socket = new Socket(serverIp, serverPort);
				serverReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				pw = new PrintWriter(socket.getOutputStream(), true);
				pw.println(userCommand);
				log.info("Message flushed to leader");
				
				pw.close();
				serverReader.close();
				socket.close();

			}
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				log.error(e);
				e.printStackTrace();
			}
		}
		else if(userCommand.startsWith("end:delete"))
		{
			// end:delete command to signal the master that file delete is done he should update the list.
			try 
			{
				// logic to ping the master and get the list of ip's
				socket = new Socket(serverIp, serverPort);
				serverReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				pw = new PrintWriter(socket.getOutputStream(), true);
				pw.println(userCommand);
				log.info("Message flushed to leader");
				
				pw.close();
				serverReader.close();
				socket.close();

			}
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				log.error(e);
				e.printStackTrace();
			}
		}
		else if(userCommand.startsWith(("trans")))
		{
			// trans command to put a file from sender to receiver.
			try 
			{
				String ip[] = userCommand.split(":");
				String senderIp = null;

				senderIp = ip[1];
				
				socket = new Socket(senderIp, serverPort);
				serverReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				pw = new PrintWriter(socket.getOutputStream(), true);
				pw.println(userCommand+":"+fileName);
				log.info("Message flushed to leader");
				String returnStr = "";
				// TODO wait logic
				while ((returnStr = serverReader.readLine()) != null) 
				{
					log.info(returnStr);
					if(returnStr.equalsIgnoreCase("OK"))
					{
						System.out.println("file trasn is completed");
					}
				}
				
				if(ip.length == 4)
					updateFileListAfterReplica(fileName, ip[3], ip[2]);
				
				pw.close();
				serverReader.close();
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
	
	public void updateFileListAfterReplica(String filename, String ipDelete, String ipNew)
	{
		for (HashMap.Entry<String, List<String>> record : Node._fileMap.entrySet()) 
		{
			if(record.getKey().equals(filename))
			{
				List<String> newIpList = new ArrayList<String>();
				for(String ip : record.getValue())
				{
					if(ip != ipDelete)
					{
						newIpList.add(ip);
					}
					else
					{
						newIpList.add(ipNew);
					}
				}
				Node._fileMap.remove(filename);
				Node._fileMap.put(filename, newIpList);
			}
			else if(record.getKey().equals("msg#"))
			{
				
				String msgCounter = record.getValue().get(0);
				int count =Integer.valueOf(msgCounter);
				count++;
				record.getValue().set(0, String.valueOf(count));
				Node._fileMsgCounter = Integer.valueOf(count);
			}
		}
	}

	public String getUserCommand() {
		return userCommand;
	}

}
