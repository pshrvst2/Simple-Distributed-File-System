import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
 *@Info Runs every two seconds to check whether the introducer is in the list or not. IF not, then it 
 * tries to set up the connection with him. 
 */
public class IntroducerRejoinThread extends Thread 
{

	public Logger _logger = Logger.getLogger(IntroducerRejoinThread.class);

	public IntroducerRejoinThread() {
		// TODO Auto-generated constructor stub
	}

	public void run()
	{
		boolean itroducerAlive = false;
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			String nodeId = record.getKey();
			if(nodeId.startsWith(Node._introducerIp))
			{
				itroducerAlive = true;
			}
		}

		if(!itroducerAlive)
			pingIntroducer();

	}

	public void pingIntroducer()
	{
		_logger.info("Ping Introducer begins.");
		DatagramSocket socket = null;
		try
		{
			socket = new DatagramSocket();
			int length = 0;
			byte[] buf = null;

			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objOpStream = new ObjectOutputStream(byteArrayOutputStream);
			HashMap<String, NodeData> map = new HashMap<String, NodeData>();
			for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
			{
				map.put(record.getKey(), record.getValue());
			}
			objOpStream.writeObject(map);
			buf = byteArrayOutputStream.toByteArray();
			length = buf.length;

			DatagramPacket dataPacket = new DatagramPacket(buf, length);
			dataPacket.setAddress(InetAddress.getByName(Node._introducerIp));
			dataPacket.setPort(Node._gossipMemberListPort);

			socket.send(dataPacket);
			
			byteArrayOutputStream.close();			
			objOpStream.close();
		}
		catch(SocketException ex)
		{
			_logger.error(ex);
			//ex.printStackTrace();
		}
		catch(IOException ioExcep)
		{
			_logger.error(ioExcep);
			//ioExcep.printStackTrace();
		} 
		finally
		{
			if(socket != null)
				socket.close();
			_logger.info("Exiting from the method pingIntroducer.");
		}
	}
}
