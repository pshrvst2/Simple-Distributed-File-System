import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
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
	private String serverIpAddress = null;
	private Socket clientSocket = null;
	private final String localFilePath = "/home/local/";
	private final String sdfsFilePath = "/home/sdfs/";
	
	public ReqListenerInstance(Socket clientSocket, String ip) 
	{
		log.info("Connection established at socket = " + clientSocket);
		this.clientSocket = clientSocket;
		this.serverIpAddress = ip;
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
					File file = new File(sdfsFilePath+words[2]);
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
					FileReader fileReader = new FileReader(sdfsFilePath+words[2]);
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
					String deleteCmd = "rm -rf "+words[2];
					Process proc = rt.exec(new String[] { "bash", "-c", deleteCmd });
					int exitValue = proc.exitValue();
					pw.println("OK");
				}
				
			}
			else if(clientCommand.startsWith("end"))
			{
				// its a file operation
				if(words[1].equalsIgnoreCase("put"))
				{
					
				}
				else if(words[1].equalsIgnoreCase("get"))
				{
					
				}
			}
			else
			{
				// its not a file operation. Mostly this request is for leader.
				if(words[0].equalsIgnoreCase("put"))
				{
					if(Node._fileMap.containsKey(words[1]))
					{
						// replica's already exist. 
						// return String "NA"
						pw.println("NA");
					}
					else
					{
						// randomly send 3 ip addresses to the request node.
						Set<String> ipSet = getrandom3IpAddresses();
						if(ipSet!=null)
						{	
							for(String ip : ipSet)
							{
								pw.println(ip);
							}
						}
					}
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
						pw.println(ip.get(0)); // later the change the logic to get(random)
					}
				}
				else if(words[0].equalsIgnoreCase("delete"))
				{
					if(!Node._fileMap.containsKey(words[1]))
					{
						pw.println("NA");
					}
					else
					{
						List<String> ip = Node._fileMap.get(words[1]);
						pw.println(ip.get(0));
						pw.println(ip.get(1));
						pw.println(ip.get(2));
					}
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

}
