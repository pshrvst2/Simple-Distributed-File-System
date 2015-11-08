import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author pshrvst2
 *
 */
public class ReqSender extends Thread 
{
	private static Logger log = Logger.getLogger(ReqSender.class);
	private final String userCommand;
	private final String fileName;
	
	public ReqSender(String cmd, String file)
	{
		this.userCommand = cmd;
		this.fileName = file;
	}

	public void run()
	{
		log.info("User command is : "+userCommand+" "+fileName);
		if(userCommand.equalsIgnoreCase("put"))
		{
			//first check for the file at local
		}
		
	}

	public String getUserCommand() {
		return userCommand;
	}

}
