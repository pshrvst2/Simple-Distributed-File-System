import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.Logger;




public class CoordinatorMessageThread extends Thread
{
	public static Logger _logger = Logger.getLogger(CoordinatorMessageThread.class);
	private int port;
	private String coordinatorId;
	//private List<String> ipList = new ArrayList<String>();
	
	public CoordinatorMessageThread(int port, String id) 
	{
		this.port = port;
		this.coordinatorId = id;
	}
	
	public void run()
	{
		_logger.info("CoordinatorMessageThread initialzing....");
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			//TODO should reconsider this 
			// initial design, includes itself and update the isleader only through the listner
			String ip = record.getKey().substring(0, record.getKey().indexOf(":"));
			Thread coordinatorThread = new CoordinatorSenderThread(port,ip,coordinatorId);
			coordinatorThread.start();
		}
	}
	
	
	public class CoordinatorSenderThread extends Thread
	{
		private int port;
		private String serverhost;
		private String coordinatorId;
		public CoordinatorSenderThread(int port, String ip, String id)
		{
			this.port = port;
			this.serverhost =ip;
			this.coordinatorId = id;
		}
		public void run()
		{
			try 
			{
				Socket socket = new Socket(serverhost, port);
				BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				out.println(Node._coordinatorMessage+"["+coordinatorId+"]");
				
				String servermsg ="";
				while((servermsg = in.readLine()) !=null)
				{
					_logger.info("Coordniator message has been send out and the server side returns : "+ servermsg);
				}
				
				out.close();
				in.close();
				socket.close();
				
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
	}

}
