import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author pshrvst2
 * @INFO Class to scan the membership thread and perform necessary actions.
 *
 */
public class ListScanThread extends Thread {

	public Logger _logger = Logger.getLogger(ListScanThread.class);
	public int leaderCount =0;

	public ListScanThread() 
	{
		// Default constructor : Do nothing
	}

	public void run()
	{
		//_logger.info("ListScanThread is activated! Listening started");
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			String nodeId = record.getKey();				
			if(!nodeId.equalsIgnoreCase(Node._machineId))
			{
				if(!record.getValue().isActive() & ((System.currentTimeMillis() - record.getValue().getLastRecordedTime()) >= Node._TCleanUpInMilliSec))
				{
					Node._gossipMap.remove(nodeId);
					_logger.info("Deleting the machine: "+nodeId+" from the membership list! at time "
							+System.currentTimeMillis());
					
				}
				else if(record.getValue().isActive() & ((System.currentTimeMillis() - record.getValue().getLastRecordedTime()) >= Node._TfailInMilliSec))
				{
					Node._gossipMap.get(nodeId).setActive(false);
					Node._gossipMap.get(nodeId).setLastRecordedTime(System.currentTimeMillis());
					_logger.info("Marking the machine: "+nodeId+" Inactive or dead in the membership list! at time "
							+ System.currentTimeMillis());			
					Node._lossCounts++;
				}
			}
		}
		//_logger.info("ListScanThread is activated! Listening ends");
		Node._totalCounts++;
		//_logger.info("\t"+"Total counts: "+ Node._totalCounts + " || loss counts: "+ Node._lossCounts);
	}

}
