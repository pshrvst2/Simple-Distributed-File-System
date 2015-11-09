import java.util.ArrayList;
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
		// leader check is there any leftout on the replica
		// happen when the old leader die and new leader need to do the replica
		String leaderId = Node.getLeadId();
		if(leaderId !=null)
		{		
			if(Node._machineId.equals(leaderId))
			{
				// do the update here and clean the hash map 
				if(!Node._fileReplicaMap.isEmpty())
				{
					
					for (HashMap.Entry<String, List<String>> r : Node._fileReplicaMap.entrySet())
					{
						// the form here is (ipDeleted, ipOLd, ipNew)
						String comnd = "trans:"+r.getValue().get(1)+":"+r.getValue().get(2)+":"+r.getValue().get(0);
						Thread fileOprReqInstance = new ReqSender(comnd, r.getKey(), Node.getLeadIp(), Node._TCPPortForRequests);
						fileOprReqInstance.start();
						while(fileOprReqInstance.isAlive())
						{
							// do nothing, just wait
						}
						
					}
					// do update here and clean up the map 
					Node._fileReplicaMap.clear();
				}
			}
			Node._hasLeader = true;
			// new leader up and if you r the leader, u will handle the replica above
			// if you r not the leader, u don't care the replica anymore. 
			Node._fileReplicaMap.clear();
		}
		
		//_logger.info("ListScanThread is activated! Listening started");
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet())
		{
			String nodeId = record.getKey();				
			if(!nodeId.equalsIgnoreCase(Node._machineId))
			{
				if(!record.getValue().isActive() & ((System.currentTimeMillis() - record.getValue().getLastRecordedTime()) >= Node._TCleanUpInMilliSec))
				{
					Node._gossipMap.remove(nodeId);
					// if this machine detect the failure, log the file name and ip addr in the list
					// should clean up the list when leader change

					
					// if you r the leader, send out the put request to replica the file 
					if(Node._machineId.equals( Node.getLeadId()))
					{
						//do the update here and clean the hash map 
						
						if(Node._gossipMap.size()>=3)
						{
							HashMap<String, List<String>> newMap = getReplicaMap(nodeId);
							for (HashMap.Entry<String, List<String>> r : newMap.entrySet())
							{							
								// the form here is (ipDeleted, ipOLd, ipNew)
								String comnd = "trans:"+r.getValue().get(1)+":"+r.getValue().get(2)+":"+r.getValue().get(0);
								Thread fileOprReqInstance = new ReqSender(comnd, r.getKey(), Node.getLeadIp(), Node._TCPPortForRequests);
								fileOprReqInstance.start();
								while(fileOprReqInstance.isAlive())
								{
									// do nothing, just wait
								}
							}
						}
						
						Node._fileReplicaMap.clear();
						//should do the leader change here in the mesg# [0,0,0] the second one is the counts of leader change 
						
					}
					// if the leader fail, keep an copy of his file and wait for the new leader to do the replica
					else if(record.getValue().isLeader() == true)
					{
						Node._hasLeader = false;
						// we only do replica when we have three members
						if(Node._gossipMap.size()>=3)
						{
							HashMap<String, List<String>> newMap = getReplicaMap(nodeId);
							for (HashMap.Entry<String, List<String>> r : newMap.entrySet())
							{
								Node._fileReplicaMap.put(r.getKey(), r.getValue());
							}
						}
					}
					
					
					
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

	// only do this when we have 3 or more members after delete 
	// Map look like this  <Key: filename, List(ipDeleted, ipOld, ipNew)>
	public HashMap<String, List<String>> getReplicaMap(String ip)
	{
		HashMap<String, List<String>> map = new HashMap<String, List<String>>();
		
		for (HashMap.Entry<String, List<String>> record : Node._fileMap.entrySet()) 
		{
			 // check if the file store in this ip address
			 if(record.getValue().contains(ip))
			 {
				 List<String> ipList = record.getValue();
				 String newReplicaIp = getANewReplicaIp(ipList);
				 String oldReplicaIp = null;
				 for(String existip : ipList)
				 {
					 if(existip != ip)
					 {
						 oldReplicaIp= existip;
						 break;
					 }
				 }
				 // the form here is (ipDeleted, ipOLd, ipNew)
				 List<String> replicaList = new ArrayList<String>();
				 replicaList.add(ip);
				 replicaList.add(oldReplicaIp);
				 replicaList.add(newReplicaIp);
				 // the form here is  <Key: filename, List(ipDeleted, ipOld, ipNew)>
				 // Step 1: get file from ipOld and put it to ipNew
				 // Step 2: update the fileList by replacing ipDeleted by ipNew
				 map.put(record.getKey(), replicaList);
			 }
			 
		}
		
		return map;
	}
	
	public String getANewReplicaIp(List<String> existIpList)
	{
		String newIp = null;
		
		for (HashMap.Entry<String, NodeData> record : Node._gossipMap.entrySet()) 
		{
			// check if the ip inside the list, if not use this as a new ip
			if (!existIpList.contains(record.getKey().substring(0, record.getKey().indexOf(":"))))
			{
				newIp= record.getKey().substring(0, record.getKey().indexOf(":"));
				break;
			}
		}
		
		return newIp;
	}
	

	public void updateFileListAfterReplica(String filename, String ipDelete, String ipNew)
	{
		for (HashMap.Entry<String, List<String>> record : Node._fileMap.entrySet()) 
		{
			if(record.getKey().equals(filename))
			{
				List<String> newIpList = new ArrayList<String>();
				for(String ip : record.getValue())
				{
					if(ip != ipDelete)
					{
						newIpList.add(ip);
					}
					else
					{
						newIpList.add(ipNew);
					}
				}
				Node._fileMap.remove(filename);
				Node._fileMap.put(filename, newIpList);
			}
			else if(record.getKey().equals("msg#"))
			{
				
				String msgCounter = record.getValue().get(0);
				int count =Integer.valueOf(msgCounter);
				count++;
				record.getValue().set(0, String.valueOf(count));
				Node._fileMsgCounter = Integer.valueOf(count);
			}
		}
	}
}
