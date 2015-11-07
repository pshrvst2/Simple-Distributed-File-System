import java.io.Serializable;

public class NodeData implements Serializable
{

	private static final long serialVersionUID = 1L;
	
	
	
	private String nodeId = "";
	private long heartBeat = 0l;
	private long lastRecordedTime = 0l;
	private boolean isActive = true;
	//TODO may re-consider this later 
	// code block for the election 
	private int pid = 99;
	private int electionCounts = 0;
	private int okMessageCounts = 0;
	private boolean isLeader = false;
	
	public NodeData() 
	{
		super();
	}
	
	public NodeData(String nodeId, long heartBeat, long lastRecordedTime) 
	{
		//super();
		this.nodeId = nodeId;
		this.heartBeat = heartBeat;
		this.lastRecordedTime = lastRecordedTime;
	}

	public NodeData(String nodeId, long heartBeat, long lastRecordedTime,
			boolean isActive) 
	{
		this.nodeId = nodeId;
		this.heartBeat = heartBeat;
		this.lastRecordedTime = lastRecordedTime;
		this.isActive = isActive;
	}
	
	public NodeData(String nodeId, long heartBeat, long lastRecordedTime,
			boolean isActive, int pid) 
	{
		this.nodeId = nodeId;
		this.heartBeat = heartBeat;
		this.lastRecordedTime = lastRecordedTime;
		this.isActive = isActive;
		this.pid = pid;
	}
	
	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	public long getHeartBeat() {
		return heartBeat;
	}
	public void setHeartBeat(long heartBeat) {
		this.heartBeat = heartBeat;
	}
	public void increaseHeartBeat()
	{
		this.heartBeat += 1;
	}
	public long getLastRecordedTime() {
		return lastRecordedTime;
	}
	public void setLastRecordedTime(long lastRecordedTime) {
		this.lastRecordedTime = lastRecordedTime;
	}
	public boolean isActive() {
		return isActive;
	}
	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}
	
	
	public int getPid()
	{
		return pid;
	}
	public void setPId(int id)
	{	
		this.pid = id;
	}
	public int getElectionCounts()
	{
		return electionCounts;
	}
	public void setElectionCounts(int count)
	{	
		this.electionCounts = count;
	}
	public void increaseElectionCounts()
	{
		this.electionCounts +=1;
	}
	public int getOkMessageCounts()
	{
		return okMessageCounts;
	}
	public void setOkMessageCounts(int count)
	{	
		this.okMessageCounts = count;
	}
	public void increaseOkMessageCounts()
	{
		this.okMessageCounts +=1;
	}
	public boolean isLeader()
	{
		return isLeader;
	}
	public void setIsLeader(boolean l)
	{
		this.isLeader = l;
	}
}
