import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

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
	private final String localFilePath = "/home/local/";
	private final String sdfsFilePath = "/home/sdfs/";
	
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
			String fullFilePath = localFilePath+fileName;
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
				
				if(!listOfIp.isEmpty())
				{
					for(String ip : listOfIp)

					{
						if(!ip.equalsIgnoreCase(serverIp))
						{
							Socket fileTransferSocket = new Socket(ip, serverPort);
							FileReader fileReader = new FileReader(fullFilePath);
							PrintWriter filePw = new PrintWriter(fileTransferSocket.getOutputStream(), true);
							filePw.println("begin:"+userCommand+":"+fileName);
							BufferedReader bufReader = new BufferedReader(fileReader);

							while((line = bufReader.readLine()) != null)
							{
								filePw.println(line);
								System.out.println(line); 
							}
							bufReader.close();
							filePw.close();
							fileTransferSocket.close();
						}
						else
						{
							// Leader want the replica at its sdfs
							pw.println("begin:"+userCommand+":"+fileName);
							FileReader fileReader = new FileReader(fullFilePath);
							BufferedReader bufReader = new BufferedReader(fileReader);
							while((line = bufReader.readLine()) != null)
							{
								pw.println(line);
								System.out.println(line); 
							}
							bufReader.close();
						}
					}
					// send the final ack to leader that operation is done
					pw.println("end:"+userCommand+":"+fileName);
				}
				else
				{
					System.out.println("We already have replica's");
				}
				
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
		
		else if(userCommand.equalsIgnoreCase("get"))
		{
			// get file from SDFS
			String fullFilePath = sdfsFilePath+fileName;
			String line = null;
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
				
				if(!remoteIp.equals("NF"))
				{
					File file = new File(fullFilePath);
					file.createNewFile();
					PrintWriter resultWriter = new PrintWriter(file);
					if(!remoteIp.equalsIgnoreCase(serverIp))
					{
						Socket fileTransferSocket = new Socket(remoteIp, serverPort);
						PrintWriter filePw = new PrintWriter(fileTransferSocket.getOutputStream(), true);
						filePw.println("begin:"+userCommand+":"+fileName);
						BufferedReader bufReader = new BufferedReader(new InputStreamReader(fileTransferSocket.getInputStream()));
						
						while((line = bufReader.readLine()) != null)
						{
							resultWriter.println(line);
							System.out.println(line);
						}
						bufReader.close();
						filePw.close();
						fileTransferSocket.close();
					}
					else
					{
						// Leader sent its own location
						pw.println("begin:"+userCommand+":"+fileName);
						while((line = serverReader.readLine()) != null)
						{
							resultWriter.println(line);
							System.out.println(line); 
						}
					}
					resultWriter.close();
				}
				else
				{
					System.out.println("No such file at SDFS");
				}
				pw.println("end:"+userCommand+":"+fileName);
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
			String fullFilePath = sdfsFilePath+fileName;
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
					listOfIp.add(returnStr);
				}
				int operationCount = 0;
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
						pw.println("begin:"+userCommand+":"+fullFilePath);
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
				
				// send the final ack to leader that operation is done
				if(operationCount == 3)
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
		
	}

	public String getUserCommand() {
		return userCommand;
	}

}