import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author pshrvst2
 * @Info Listen to other Peers through Gossip protocol.
 *
 */
public class GossipListenerThread extends Thread 
{
	public static Logger _logger = Logger.getLogger(GossipListenerThread.class);
	private int port;
	/**
	 * 
	 */
	public GossipListenerThread(int port) 
	{
		this.port = port;
	}

	public void run()
	{
		//_logger.info("Listener thread is activated! Listening ....");
		byte[] data = new byte[4096];
		DatagramSocket listernerSocket;
		try 
		{
			listernerSocket = new DatagramSocket(port);
			while(!Node._gossipListenerThreadStop)
			{
				try 
				{
					DatagramPacket receivedPacket = new DatagramPacket(data, data.length);
					listernerSocket.receive(receivedPacket);
					int port = receivedPacket.getPort();
					InetAddress ipAddress = receivedPacket.getAddress();
					_logger.info("Received packet from: "+ipAddress+" at port: "+port);

					byte[] receivedBytes = receivedPacket.getData();
					ByteArrayInputStream bais = new ByteArrayInputStream(receivedBytes);
					ObjectInputStream objInpStream = new ObjectInputStream(bais);
					@SuppressWarnings("unchecked")
					HashMap<String, NodeData> map = (HashMap<String, NodeData>) objInpStream.readObject();


					for (HashMap.Entry<String, NodeData> record : map.entrySet())
					{

						String machineId = record.getKey().trim();
						_logger.info("******machineId = "+machineId+" Heartbeat = "+record.getValue().getHeartBeat()+" ****************");
						// to prevent the machine accidentally mark itself dead just because other say so. 
						if(!machineId.equalsIgnoreCase(Node._machineId))
						{
							Thread updateThread = new MemberUpdateThread(machineId, record.getValue());
							updateThread.start();
						}
						// update your pid from the introducer					
						else if(!Node._isIntroducer)
						{
							if(Node._introducerIp.equalsIgnoreCase(ipAddress.toString()) & Node._gossipMap.get(Node._machineId).getPid()==99)
							{
								Node._gossipMap.get(Node._machineId).setPId(record.getValue().getPid());
							}						
						}
					}

				}
				catch (IOException e) 
				{
					_logger.error(e);
					e.printStackTrace();
				}
				catch (ClassNotFoundException e) 
				{
					_logger.error(e);
					e.printStackTrace();
				}                
			}
		}
		catch (SocketException e1)
		{
			_logger.error(e1);
			e1.printStackTrace();
		}
	}

	public class MemberUpdateThread extends Thread 
	{
		public Logger _logger = Logger.getLogger(MemberUpdateThread.class);
		private String id = "";
		private NodeData nodeData = null;

		public MemberUpdateThread(String id, NodeData record)
		{
			this.id = id;
			this.nodeData = record;
		}

		public void run()
		{
			// Every record has its thread to check for the updates.
			// Case when the member is still alive on the received list.
			if(nodeData.isActive())
			{
				//add the new member
				if(!Node._gossipMap.containsKey(id))
				{
					_logger.info("Added a new machine: "+id);
					
					// Assign a new pid for the new member
					if(Node._isIntroducer)
					{
						int temp = 0;
						for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
						{
							if(record.getValue().getPid()>temp)
							{
								temp = record.getValue().getPid();
							}
						}
						nodeData.setPId(temp+1);
					}
					Node._gossipMap.put(id, nodeData);
					Node._gossipMap.get(id).setLastRecordedTime(System.currentTimeMillis());
					
				}
				// heartbeat of the process is more than the local copy's heartbeart. That means the process has
				// communicated to other processes in the group that it's alive! Don't kill me plssss!!
				else if(nodeData.getHeartBeat() > Node._gossipMap.get(id).getHeartBeat())
				{
					Node._gossipMap.get(id).increaseHeartBeat();
					Node._gossipMap.get(id).setLastRecordedTime(System.currentTimeMillis());
					Node._gossipMap.get(id).setActive(true);
				}
				// check for this process. is it inactive for a long time? Should I declare it dead?
				else if(System.currentTimeMillis() - Node._gossipMap.get(id).getLastRecordedTime()
						> Node._TfailInMilliSec)
				{
					Node._gossipMap.get(id).setActive(false);
					Node._gossipMap.get(id).setLastRecordedTime(System.currentTimeMillis());
					// use the above set time to delete the member from the list. This should be done by a different class or thread possibly.
				}

			}
			// case when the received list has the member as dead.
			else
			{
				NodeData localCopy = Node._gossipMap.get(id);
				
				// Just in case a dead member hasn't introduce to you before, we need to make sure we will not update our member list with this info
				if(localCopy != null)
				{
					// Consider a scenario: Process sent a heartbeat of "15 and dead" and local has "14 and alive". Clearly, mark him dead!
					// Consider a scenario: Process sent a heartbeat of "14 and dead" and local has "14 and alive". Don't do anything but wait for _TFail. But it may also 
					// 						happen that this was the last info sent regarding this process. So, have a check in scheduler or thread class. 
					if(localCopy.isActive() & (localCopy.getHeartBeat() < nodeData.getHeartBeat()))
					{
						// TODO clash of thoughts here. Piyush wants an additional
						// check on the heartbeat, Kevin disagrees.
						_logger.info("Marking machine id: "+id+ " as Inactive (dead)");
						Node._gossipMap.get(id).setActive(false);
						Node._gossipMap.get(id).setLastRecordedTime(System.currentTimeMillis());
						// We are updating this so that we can compare it with _TCleanUp.
					}
				}
			}
		}
	}
}

