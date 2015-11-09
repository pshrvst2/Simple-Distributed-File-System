import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.log4j.Logger;


public class ElectionListenerThread extends Thread 
{
	public static Logger _logger = Logger.getLogger(ElectionListenerThread.class);
	private int port;
	
	public ElectionListenerThread(int port) 
	{
		this.port = port;
	}

	public void run()
	{
		_logger.info("ElectionListenerThread initialzing....");
		ServerSocket listener = null;
		try 
		{
			listener = new ServerSocket(port);
			
            while (!Node._electionListenerThreadStop) 
            {
               // new MessageHandlerThread(listener.accept()).start();
            	Socket socket = listener.accept();
            	BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String message = in.readLine();
				_logger.info("ElectionListenerThread received message : "+message);
				if(message == null || message.isEmpty())
				{
					// do nothing? 
				}
				// update the leader if receive the coordinator message
				else if(message.contains(Node._coordinatorMessage))
				{
					String id = message.substring(message.indexOf("[")+1, message.indexOf("]"));
					if(Node._gossipMap.containsKey(id))
					{
						Node._gossipMap.get(id).setIsLeader(true);
					}								
					else
					{
						// TODO message out ??
					}
				}
				// if its id is the lowest, reply ok and send out the coordinator message 
				// else reply ok and send out the election message
				else if(message.contains(Node._electionMessage))
				{
					String id = message.substring(message.indexOf("[")+1, message.indexOf("]"));
					List<String> idList = Node.getLowerIdList(Node._machineId);
					if(idList.isEmpty()||idList.size() ==0)
					{
						sendOkMessage(id);
						// call coordinate thread 
						Thread coordinatorThread = new CoordinatorMessageThread(Node._TCPPortForElections,Node._machineId );
						coordinatorThread.start();
						/*Thread okMsgThread =  new OkMessageThread(Node._TCPPort,id);
						okMsgThread.start();*/
					}
					else
					{
						// call election thread
						Thread electionThread = new ElectionSenderThread(idList, Node._TCPPortForElections);
						electionThread.start();
						sendOkMessage(id);
						/*Thread okMsgThread =  new OkMessageThread(Node._TCPPort,id);
						okMsgThread.start();*/
					}
				}
				else if (message.contains(Node._okMessage))
				{
					_logger.info("Received OK message");
					Node._gossipMap.get(Node._machineId).increaseOkMessageCounts();
					Node._gossipMap.get(Node._machineId).setIsLeader(false);
				}
				socket.close();
            }              	           
        } 
		catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
        finally 
        {
				try 
				{
					listener.close();
				} 
				catch (IOException e) 
				{
					_logger.error(e.getMessage());
			
				}
        }
	}

	/*private class MessageHandlerThread extends Thread
	{
		private Socket socket;
		
		public MessageHandlerThread(Socket socket)
		{
			this.socket = socket; 
		}
		
		public void run()
		{
			try
			{
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				PrintWriter out = new PrintWriter(socket.getOutputStream());
				
				while(true)
				{
					String message = in.readLine();
					_logger.info("ElectionListenerThread received message : "+message);
					if(message == null || message.isEmpty())
					{
						break;
					}
					// update the leader if receive the coordinator message
					else if(message.contains(Node._coordinatorMessage))
					{
						String id = message.substring(message.indexOf("[")+1, message.indexOf("]"));
						if(Node._gossipMap.containsKey(id))
						{
							Node._gossipMap.get(id).setIsLeader(true);
						}								
						else
						{
							// TODO message out ??
						}
						
					}
					// if its id is the lowest, reply ok and send out the coordinator message 
					// else reply ok and send out the election message
					else if(message.contains(Node._electionMessage))
					{
						String id = message.substring(message.indexOf("[")+1, message.indexOf("]"));
						List<String> idList = Node.getLowerIdList(Node._machineId);
						if(idList.isEmpty()||idList.size() ==0)
						{
							// call coordinate thread 
							Thread coordinatorThread = new CoordinatorMessageThread(Node._portReceiver,Node._machineId );
							coordinatorThread.start();
							sendOkMessage(id);
							Thread okMsgThread =  new OkMessageThread(Node._TCPPort,id);
							okMsgThread.start();
						}
						else
						{
							// call election thread
							Thread electionThread = new ElectionSenderThread(Node._TCPPort,idList);
							electionThread.start();
							sendOkMessage(id);
							Thread okMsgThread =  new OkMessageThread(Node._TCPPort,id);
							okMsgThread.start();
							
						}
					}
					// TODO ok message should not be handler herer
					else if(message == Node._okMessage)
					{
						//TODO
					}
				}
			}
			catch(Exception e)
			{
				_logger.error(e.getMessage());
			}
			
		}
		
	}*/
	
	public void sendOkMessage(String ip) throws UnknownHostException, IOException
	{
		_logger.info("Sending OK message to: "+ip);
		String temp[] = ip.split(":");
		Socket socket = new Socket(temp[0], Node._TCPPortForElections);
		BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
		PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
		out.println(Node._okMessage);
		
		String servermsg ="";
		while((servermsg = in.readLine()) !=null)
		{
			_logger.info("Coordniator message has been send out and the server side returns : "+ servermsg);
		}
		
		out.close();
		in.close();
		socket.close();
	}
	
}
