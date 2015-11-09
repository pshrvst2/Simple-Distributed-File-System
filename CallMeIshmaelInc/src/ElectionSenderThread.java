import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;


import org.apache.log4j.Logger;


public class ElectionSenderThread extends Thread
{
	public static Logger _logger = Logger.getLogger(ElectionSenderThread.class);
	private int port;
	private List<String> idList = new ArrayList<String>();

	public ElectionSenderThread(List<String> list, int port) 
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
				try
				{
					_logger.info("Sending election message to machine: "+id);
					String serverhost = id.substring(0, id.indexOf(":")).trim();
					Socket socket = new Socket(serverhost, port);
					//_logger.info(Node._machineId + " is connected at port: " + String.valueOf(port));
					BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					out.println(Node._electionMessage+"["+Node._machineId+"]");
					
					/*String servermsg = "";
					while ((servermsg = in.readLine()) !=null)
					{
						// check ok message 
						if(servermsg.contains(Node._okMessage))
						{
							Node._gossipMap.get(Node._machineId).increaseOkMessageCounts();
							break;
						}
					}*/
					
					out.close();
					in.close();
					socket.close();
					
					
					/*DatagramSocket socket = new DatagramSocket();
					int length = 0;
					byte[] buf = null;
					String serverhost = id.substring(0, id.indexOf(":")).trim();	
					
					ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
				    ObjectOutputStream objOpStream = new ObjectOutputStream(byteArrayOutputStream);
				    //objOpStream.writeObject(_gossipList);
				    objOpStream.writeObject(Node._electionMessage+"["+Node._machineId+"]");
				    buf = byteArrayOutputStream.toByteArray();
				    length = buf.length;
				    
				    DatagramPacket dataPacket = new DatagramPacket(buf, length);
					dataPacket.setAddress(InetAddress.getByName(serverhost));
					//dataPacket.setPort(port);
					dataPacket.setPort(Node._portSender);
					int retry = 1;
					//try three times as UDP is unreliable. At least one message will reach :)
					while(retry > 0)
					{
						socket.send(dataPacket);
						--retry;
					}
					socket.close();*/
				}
				catch (SocketException e) 
				{
					// TODO Auto-generated catch block
					//e.printStackTrace();
				}
				catch(IOException ioExcep)
				{
					_logger.error(ioExcep);
					//ioExcep.printStackTrace();
				}
			}
			Node._gossipMap.get(Node._machineId).increaseElectionCounts();
		}	
	}
	
	/*public class ElectionMessageThread extends Thread 
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
						break;
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
	}*/

}
