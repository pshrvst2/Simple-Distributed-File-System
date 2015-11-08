import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

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
		if(Node._gossipMap.keySet().size() > 1)
		{
			//_logger.info("ListScanThread is activated! Listening started");

			for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
			{
				if(record.getValue().isLeader())
				{
					leaderCount++;
					break;
				}
			}

			//check the existence of the leader
			if(leaderCount == 0 )
			{
				//check the election counts
				if(Node._gossipMap.get(Node._machineId).getElectionCounts() == 0 )
				{
					// clean up the ok message counts since the previous election is done!
					Node._gossipMap.get(Node._machineId).setOkMessageCounts(0);
					// start a new election 
					List<String> candidateIds = Node.getLowerIdList(Node._machineId);
					if(!candidateIds.isEmpty())
					{
						_logger.info("Sending Election message");
						Thread electionThread = new ElectionSenderThread(candidateIds, Node._TCPPort);
						electionThread.start();
					}
				}
				// An election has been taken place, let see whether we receive any ok messages 
				else
				{
					if (Node._gossipMap.get(Node._machineId).getOkMessageCounts() == 0)
					{
						// send out the coordinate message here, since the election has taken place
						// and no ok message return, we consider this node as leader
						Node._gossipMap.get(Node._machineId).setElectionCounts(0);
						Thread coordinatorThread = new CoordinatorMessageThread(Node._TCPPort,Node._machineId);
						coordinatorThread.start();	
					}
					else
					{
						// since we have received one or more ok message, we should end the election and 
						// wait for the coordinate message from other lowest pid node
						Node._gossipMap.get(Node._machineId).setElectionCounts(0);
					}
				}

			}
			else
			{
				//reset the count
				leaderCount =0;
				
				/*********************************************************************
				 * A test block, need to remove this after testing the file list Threads 
				 *********************************************************************/		
				if(Node._machineId == Node.getLeadId())
				{
					
					
					if(Node._fileMap.isEmpty())
					{
						String messageCounts = "msg#";
						List<String> firstList = new ArrayList<String>();
						firstList.add("0");
						firstList.add("0");
						firstList.add("0");
						Node._fileMap.put(messageCounts, firstList);
					}
					else
					{
						Set<String> ips = FileListSenderThread.getTwoSuccessorIps(Node.getLeadId());
						if (ips.size() == 2)
						{
							// create the file list here 
							String counts = String.valueOf(++Node._fileMsgCounter);
							Node._fileMap.get("msg#").set(0, counts);
							String fileName = String.valueOf(Node._fileNameInt);
							List<String> addressList = new ArrayList<String>();
							// just because we have three mechine, so we need put itself self here 
							addressList.add(Node._machineIp);
							for( String addr : ips)
							{
								addressList.add(addr);
							}
							Node._fileMap.put(fileName, addressList);							
							Node._fileNameInt += 1;
							
							// pass the file list to others 
							Thread fileListThread = new FileListSenderThread(Node._gossipFileListPort,true);
							fileListThread.start();
						}
						
						
					}		
				}
				
			}
		}
	}

}
