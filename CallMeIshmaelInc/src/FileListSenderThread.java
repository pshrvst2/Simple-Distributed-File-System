import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;


public class FileListSenderThread extends Thread
{
	public static Logger _logger = Logger.getLogger(FileListSenderThread.class);
	private int port;
	private boolean isLeader; 
	
	public FileListSenderThread(int port, boolean isLeader)
	{
		this.port = port;
		this.isLeader = isLeader;
	}
	
	public void run() 
	{

		DatagramSocket senderSocket;
		try {
			senderSocket = new DatagramSocket();
			int length = 0;
			byte[] buf = null;

			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objOpStream = new ObjectOutputStream(byteArrayOutputStream);
			
			Set<String> ip2bSent = new HashSet<String>();
			
			// check if the machine is the leader, if it's the leader send to two of his successor
			if(isLeader)
			{
				ip2bSent = getTwoSuccessorIps(Node.getLeadId());
			}
			// if not, send to any random 2 server beside itself and the leader
			else
			{
				ip2bSent = get2IpAddresses(Node._machineId,Node.getLeadId());
			}

			HashMap<String, List<String>> map = new HashMap<String, List<String>>();
			for(HashMap.Entry<String, List<String>> record: Node._fileMap.entrySet())
			{
				map.put(record.getKey(), record.getValue());
				_logger.info("file info: file name " + record.getKey()
						+ "|| addr1: " + record.getValue().get(0)
						+ "|| addr2: " + record.getValue().get(1)
						+ "|| addr3: " + record.getValue().get(2));
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
					_logger.info("Sent file list form machine ip : " + Node._machineIp
							+ " to machine ip : " + ip);
				}
			}
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
	
	
	
	// this gonna return no more than 2 random ip except this machine's and the leader's
	public static Set<String> get2IpAddresses(String machineId,String leadId) 
	{
		HashMap<String, NodeData> gossipMap = new HashMap<String, NodeData>();
		gossipMap.putAll(Node._gossipMap);
		Set<String> ips = new HashSet<String>();
		
		// take out the local id and leader id
		int len = gossipMap.size() - 2;
		if (len != 0) 
		{
			// retrieve the ip list from membership list
			String[] retVal = new String[len];
			int i = 0;
			for (HashMap.Entry<String, NodeData> rec : gossipMap.entrySet()) 
			{
				String id = rec.getKey();
				String[] temp = id.split(":");
				if (id!= machineId & id!= leadId ) 
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
	
	//This method only called by the leader to retrieve the two successor 
	public static Set<String> getTwoSuccessorIps (String leadId)
	{
		HashMap<String, NodeData> gossipMap = new HashMap<String, NodeData>();
		gossipMap.putAll(Node._gossipMap);
		Set<String> ips = new HashSet<String>();
		List<Integer> ipList = new ArrayList<Integer>();
		HashMap<Integer,String> pidMap = new HashMap<Integer, String>();
		int len = gossipMap.size() - 1;
		if (len != 0) 
		{
			for (HashMap.Entry<String, NodeData> rec : gossipMap.entrySet()) 
			{
				String id = rec.getKey();
				if (id!= leadId ) 
				{
					ipList.add(rec.getValue().getPid());
					pidMap.put(rec.getValue().getPid(), id);
				}
			}
		}
		// sort the list from lower to higher
		Collections.sort(ipList);
		if(ipList.size()==1)
		{
			String ip = pidMap.get(ipList.get(0));
			ips.add(ip.substring(0, ip.indexOf(":")));
		}
		else if (ipList.size()>=2)
		{
			String ip1 = pidMap.get(ipList.get(0));
			ips.add(ip1.substring(0, ip1.indexOf(":")));
			String ip2 = pidMap.get(ipList.get(1));
			ips.add(ip2.substring(0, ip2.indexOf(":")));
		}
		return ips; 
	}
}
