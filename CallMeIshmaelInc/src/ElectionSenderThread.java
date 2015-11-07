import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;


public class ElectionSenderThread extends Thread
{
	public static Logger _logger = Logger.getLogger(ElectionListenerThread.class);
	private int port;
	private List<String> idList = new ArrayList<String>();

	public ElectionSenderThread(int port, List<String> list) 
	{
		this.port = port;
		this.idList = list;
	}

	public void run()
	{
		_logger.info("ElectionSenderThread initialzing....");
		if (!idList.isEmpty())
		{			
			for(String id : idList)
			{
				Thread electionMThread = new ElectionMessageThread(port, id);
				electionMThread.start();
			}
			Node._gossipMap.get(Node._machineId).increaseElectionCounts();
			// schedule a timmer check poin to check the ok message 
			ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.schedule(new CheckOkMessageThread(), Node._timeOutForElection, TimeUnit.SECONDS);
		}	
	}
	
	public class ElectionMessageThread extends Thread 
	{
		private int port;
		private String serverhost; 
		private String id;
		
		public ElectionMessageThread(int port, String id)
		{
			this.port = port;
			this.id = id;
			this.serverhost = id.substring(0, id.indexOf(":")).trim();			
		}
		
		public void run()
		{
			try 
			{
				Socket socket = new Socket(serverhost, port);
				//_logger.info(Node._machineId + " is connected at port: " + String.valueOf(port));
				BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				out.println(Node._electionMessage+"["+id+"]");
				
				String servermsg = "";
				while ((servermsg = in.readLine()) !=null)
				{
					// check ok message 
					if(servermsg.contains(Node._okMessage))
					{
						Node._gossipMap.get(Node._machineId).increaseOkMessageCounts();
					}
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
	
	public class CheckOkMessageThread extends Thread 
	{
		String id;
		public CheckOkMessageThread()
		{
			this.id = Node._machineId;
		}
		public void run()
		{
			// only take action when no ok messages received which implies your r the leader
			int eleCount = Node._gossipMap.get(id).getElectionCounts();
			int okCount = Node._gossipMap.get(id).getOkMessageCounts();
			if(Node._gossipMap.get(id).getElectionCounts()>0 & Node._gossipMap.get(id).getOkMessageCounts()==0)
			{
				Thread coordinatorThread = new CoordinatorMessageThread(Node._TCPPort,id);
				coordinatorThread.start();
			}
			// reset the ele counts and ok message counts back to 0
			Node._gossipMap.get(id).setElectionCounts(0);
			Node._gossipMap.get(id).setOkMessageCounts(0);
		}
		
	}
	
}
