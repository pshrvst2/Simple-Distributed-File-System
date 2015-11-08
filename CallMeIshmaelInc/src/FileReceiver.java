/**
 * 
 */
import java.io.IOException;
import java.net.ServerSocket;

import org.apache.log4j.Logger;
/**
 * @author pshrvst2
 *
 */
public class FileReceiver extends Thread 
{
	private static Logger log = Logger.getLogger(FileReceiver.class);
	private final int port;

	public FileReceiver(int port)
	{
		this.port = port;
	}

	public void run()
	{
		log.info("File Receiver is up! ");
		ServerSocket serverSocketListener = null;
		try 
		{
			serverSocketListener = new ServerSocket(port);
			if (serverSocketListener.equals(null))
			{
				System.out.println("FileReceiver Server socket failed to open! terminating");
				log.info("FileReceiver Server socket failed to open! terminating");
				serverSocketListener.close();
				return;
			} 
			else 
			{
				System.out.println(" File Receiver socket established, listening at port: "+port);
				log.info(" FileReceiver socket established, listening at port: "+port);
			}

			while (!Node._fileReceiverThreadStop) 
			{
				new FileReceiverInstance(serverSocketListener.accept()).start();
				log.info("Receiving a new file");
			}
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			try {
				serverSocketListener.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
