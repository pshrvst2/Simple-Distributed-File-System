import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

public class GossipSenderThread extends Thread 
{
	public static Logger _logger = Logger.getLogger(GossipSenderThread.class);
	private int port;
	private static String _machineIp;

	public GossipSenderThread(int port) 
	{
		this.port = port;
		_machineIp = Node._machineIp;
	}

	public void run() 
	{

		DatagramSocket senderSocket;
		try {
			senderSocket = new DatagramSocket();
			int length = 0;
			byte[] buf = null;

			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objOpStream = new ObjectOutputStream(
					byteArrayOutputStream);

			Set<String> ip2bSent = get2RandomIpAddresses(_machineIp);

			// update the heart-beat and time stamp before send out the
			// membership list
			updateHearbeatAndTimeStamp();
			// check if there's any member in the member list beside itself
			HashMap<String, NodeData> map = new HashMap<String, NodeData>();
			for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet()) 
			{
				map.put(record.getKey(), record.getValue());
				_logger.info("packet info: id " + record.getValue().getNodeId()
						+ " || time stamp: "
						+ record.getValue().getLastRecordedTime()
						+ " || heartbeat: " + record.getValue().getHeartBeat()
						+ " || status: " + record.getValue().isActive());
			}
			if (!ip2bSent.isEmpty()) 
			{
				objOpStream.writeObject(map);
				buf = byteArrayOutputStream.toByteArray();
				length = buf.length;

				for (String ip : ip2bSent) 
				{
					DatagramPacket dataPacket = new DatagramPacket(buf, length);
					dataPacket.setAddress(InetAddress.getByName(ip));
					dataPacket.setPort(port);
					senderSocket.send(dataPacket);
					_logger.info("Sent packet form machine ip : " + _machineIp
							+ " to machine ip : " + ip);
				}
			}
			// _logger.info("Sender thread is activated! sending ends");
		} 
		catch (SocketException e1) 
		{
			_logger.error(e1);
			e1.printStackTrace();
		} 
		catch (Exception e) 
		{
			_logger.error(e);
			e.printStackTrace();
		}

	}

	public static Set<String> get2RandomIpAddresses(String machineId) 
	{

		HashMap<String, NodeData> gossipMap = new HashMap<String, NodeData>();
		gossipMap.putAll(Node._gossipMap);
		Set<String> ips = new HashSet<String>();
		// take out the local id
		int len = gossipMap.size() - 1;
		if (len != 0) 
		{
			// retrieve the ip list from membership list
			String[] retVal = new String[len];
			int i = 0;
			for (HashMap.Entry<String, NodeData> rec : gossipMap.entrySet()) 
			{
				String machinId = rec.getKey();
				String[] temp = machinId.split(":");
				if (!temp[0].equals(machineId)) 
				{
					retVal[i] = temp[0];
					++i;
				}
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
			// when there're more than 2 member, randomly select two
			else 
			{
				while (ips.size() < 2) 
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

	public static void updateHearbeatAndTimeStamp() 
	{
		Node._gossipMap.get(Node._machineId).setLastRecordedTime(System.currentTimeMillis());
		Node._gossipMap.get(Node._machineId).increaseHeartBeat();
	}
}
