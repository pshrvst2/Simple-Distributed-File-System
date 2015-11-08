import java.io.IOException;
import java.net.ServerSocket;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author pshrvst2
 *
 */
public class ReqListener extends Thread 
{
	private static Logger log = Logger.getLogger(ReqListener.class);
	private final int port;
	private final String ip;
	
	public ReqListener(int port, String ip)
	{
		this.port = port;
		this.ip = ip;
	}
	
	public void run()
	{
		log.info("Request listener is up! ");
		ServerSocket serverSocketListener = null;
		try 
		{
			serverSocketListener = new ServerSocket(port);
			if (serverSocketListener.equals(null))
			{
				System.out.println("Server socket failed to open! terminating");
				log.info("Server socket failed to open! terminating");
				serverSocketListener.close();
				return;
			} 
			else 
			{
				System.out.println(" Req Listener socket established, listening at port: "+port);
				log.info(" Req Listener socket established, listening at port: "+port);
			}

			while (true) 
			{
				new ReqListenerInstance(serverSocketListener.accept(), ip).start();
				log.info("Listening new Req");
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
