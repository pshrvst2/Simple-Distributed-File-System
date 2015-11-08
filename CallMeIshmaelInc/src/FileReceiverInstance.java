import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author pshrvst2
 *
 */
public class FileReceiverInstance extends Thread 
{
	private static Logger log = Logger.getLogger(FileReceiverInstance.class);
	private Socket clientSocket = null;
	
	public FileReceiverInstance(Socket clientSocket) 
	{
		log.info("File transfer connection established at socket = " + clientSocket);
		this.clientSocket = clientSocket;
	}

	public void run()
	{
		try 
		{
			DataInputStream dataIpStream = new DataInputStream(clientSocket.getInputStream());
			String fileName = dataIpStream.readUTF();
            long fileSize = dataIpStream.readLong();
            String absoluteFilePath = Node.sdfsFilePath+fileName;
            File downloadedFile = new File(absoluteFilePath);
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(downloadedFile)); 
            for(int i =0;i<fileSize;i++)
            {          
            	int index;
            	while((index=dataIpStream.read())!=-1)
            	{
            		dos.write(index);
            	}
            }
            dos.close();
            dataIpStream.close();
            clientSocket.close();
            
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	 }

}
