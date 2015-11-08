import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

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
	
	public ReqListenerInstance(Socket clientSocket, String ip) 
	{
		log.info("Connection established at socket = " + clientSocket);
		this.clientSocket = clientSocket;
		this.serverIpAddress = ip;
	}
	
	public void run()
	{
		try {
			String clientCommand = "";
			BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			BufferedReader processReader = null;
			OutputStreamWriter writer = new OutputStreamWriter(clientSocket.getOutputStream());
			PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(),true);

			clientCommand = reader.readLine();
			log.info("Client fired -->" + clientCommand);
			System.out.println("Client fired -->" + clientCommand);
			
			// check what is the request about. Also if you are the leader, you need to
			// handle the requests well.
			
			if(clientCommand.startsWith("begin") || clientCommand.startsWith("end"))
			{
				// its a file operation
			}
			else
			{
				// its not a file operation. Mostly this request is for leader.
				String words[] = clientCommand.split(":");
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
					}
				}
			}
			
			Runtime rt = Runtime.getRuntime();
			
			Process proc = rt.exec(new String[] { "bash", "-c", clientCommand });

			System.out.println("The complete command is:" + clientCommand);

			String message = "";

			processReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));

			while ((message = processReader.readLine()) != null) 
			{
				
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

}
