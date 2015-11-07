import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;


public class LeaderScanThread extends Thread
{

	
	public Logger _logger = Logger.getLogger(ListScanThread.class);
	public int leaderCount =0;

	public LeaderScanThread() 
	{
		// Default constructor : Do nothing
	}
	
	public void run()
	{
		//_logger.info("ListScanThread is activated! Listening started");
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			if(record.getValue().isLeader())
			{
				leaderCount++;
			}
		}
		
		//check the existence of the leader
		if(leaderCount == 0 )
		{
			//TODO consider how to handle the introducer rejoin
			// call the election thread here
			List<String> candidateIds = Node.getLowerIdList(Node._machineId);
			Thread electionThread = new ElectionSenderThread(Node._TCPPort,candidateIds);
			electionThread.start();
		}
		else
		{
			//reset the count
			leaderCount =0;
		}
	}
	
}
