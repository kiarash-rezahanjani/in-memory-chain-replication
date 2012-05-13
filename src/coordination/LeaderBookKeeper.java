package coordination;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import utility.NetworkUtil;

import coordination.Znode.ServerData;


/**
 * Maintain data required by leader for managing in-progress operation 
 * such as forming or repairing ensemble. 
 * @author root
 *
 */
public class LeaderBookKeeper {
	private int ensembleSize = 3;

	private HashSet<InetSocketAddress> candidateSet
	= new HashSet<InetSocketAddress>();

	public HashMap<InetSocketAddress, InetSocketAddress> protocolToBufferServer
	= new HashMap<InetSocketAddress, InetSocketAddress>();
	
	private HashMap<InetSocketAddress, Boolean> requestedNodeList 
	= new HashMap<InetSocketAddress, Boolean>();

	private HashMap<InetSocketAddress, Boolean> acceptedNodeList 
	= new HashMap<InetSocketAddress, Boolean>();

	private HashMap<InetSocketAddress, Boolean> connectedNodeList 
	= new HashMap<InetSocketAddress, Boolean>();

	private List<InetSocketAddress> ensembleMembers 
	= new ArrayList<InetSocketAddress>();

	boolean ensembleConnected = false;

	public boolean isEnsembleConnected() {
		return ensembleConnected;
	}

	public void setEnsembleConnected(boolean ensembleConnected) {
		this.ensembleConnected = ensembleConnected;
	}

	/**
	 * Sets a list of all members of the ensemble including the leader.
	 * @param List of all members of the ensemble including the leader
	 */
	public void setEnsembleMembers(List<InetSocketAddress> ensembleMembers) {
		this.ensembleMembers = ensembleMembers;
	}

	/**
	 * Gets list of all members of the ensemble including the leader
	 * @return List of all members of the ensemble including the leader
	 *
	 **/
	public List<InetSocketAddress> getEnsembleMembersBufferServer() {
		List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
		for(InetSocketAddress address : ensembleMembers){
			list.add(protocolToBufferServer.get(address));
		}
		return list;
	}
	


	public List<InetSocketAddress> getEnsembleMembers() {
		return ensembleMembers;
	}
	public LeaderBookKeeper(int ensembleSize){
		this.ensembleSize=ensembleSize;
	}

	public LeaderBookKeeper(){
		this.ensembleSize=3;
	}

	public void clear()
	{
		ensembleSize = 3;
		candidateSet.clear();
		requestedNodeList.clear();
		acceptedNodeList.clear();
		connectedNodeList.clear();
	}

	/**
	 * map the protocol address to buffer server address and populate the candidate set with protocol addresses
	 * @param serverDataList
	 */
	public void setCandidatesServerData(List<ServerData> serverDataList){
		for(ServerData data : serverDataList){
			protocolToBufferServer.put(NetworkUtil.parseInetSocketAddress(data.getSocketAddress()), NetworkUtil.parseInetSocketAddress(data.getBufferServerSocketAddress()));
		}
		candidateSet.addAll(protocolToBufferServer.keySet());
	}
	
	public void setEnsembleSize(int size){
		ensembleSize = size;
	}

	public void addCandidateList(Collection<InetSocketAddress> candidates){
		candidateSet.addAll(candidates);
	}

	public void addCandidate(InetSocketAddress sa){
		candidateSet.add(sa);
	}

	public void putRequestedNode(InetSocketAddress sa, Boolean confirmed){
		requestedNodeList.put(sa, confirmed);
	}

	public void putAcceptedNode(InetSocketAddress sa, Boolean confirmed){
		acceptedNodeList.put(sa, confirmed);
	}

	void putConnectedNode(InetSocketAddress sa, Boolean confirmed){
		connectedNodeList.put(sa, confirmed);
	}

	public List<InetSocketAddress> getCandidatesProtocolAddress(){
		return new ArrayList<InetSocketAddress>(candidateSet);
	}
	/**
	 * Returns false if there is no sufficient follower have left to send connect message
	 * to form the ensemble with given replication factor.
	 * (False if it is impossible to form a ensemble with current candidates.)
	 * This method is written in generic form so that it can be used later on. But at the moment 
	 * we do not send extra request for backup. 
	 * @return
	 */
	public boolean waitForNextConnectedMessage(){
		int backup = getAcceptedList().size() - ensembleSize;
		int failed = connectedNodeList.size() - getConnectedList().size();
		if( failed > backup )
			return false;
		else
			return true;
	}


	public boolean waitForNextAcceptedMessage(){
		int backup = requestedNodeList.size() - ensembleSize;
		int failed = acceptedNodeList.size() - getAcceptedList().size();
		if( failed > backup )
			return false;
		else
			return true;
	}

	/**
	 * Get all the node that the request message has been sent.
	 * @return
	 */
	public Set<InetSocketAddress> getRequestedNodeList() {
		return requestedNodeList.keySet();
	}

	public int getEnsembleSize() {
		return ensembleSize;
	}

	/**
	 * Get the list of all the followers which accepeted the join request.
	 * @return
	 */
	public List<InetSocketAddress> getAcceptedList(){//HashMap<InetSocketAddress, Boolean>
		List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
		Iterator it = acceptedNodeList.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<InetSocketAddress, Boolean> entry = (Map.Entry<InetSocketAddress, Boolean>)it.next();
			if(entry.getValue().booleanValue()==true){
				list.add(entry.getKey());
			}
		}
		return list;
	}

	public List<InetSocketAddress> getNotAcceptedList(){//HashMap<InetSocketAddress, Boolean>
		List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
		Iterator it = acceptedNodeList.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<InetSocketAddress, Boolean> entry = (Map.Entry<InetSocketAddress, Boolean>)it.next();
			if(entry.getValue().booleanValue()==false){
				list.add(entry.getKey());
			}
		}
		return list;
	}

	/**
	 * Get the list of all the followers which have connected to their successor(in chain replication).
	 * @return
	 */
	public List<InetSocketAddress> getConnectedList(){//HashMap<InetSocketAddress, Boolean>
		List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
		Iterator it = connectedNodeList.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<InetSocketAddress, Boolean> entry = (Map.Entry<InetSocketAddress, Boolean>)it.next();
			if(entry.getValue().booleanValue()==true)
			{
				list.add(entry.getKey());
			}
		}
		return list;
	}

	public List<InetSocketAddress> getNotConnectedList(){//HashMap<InetSocketAddress, Boolean>
		List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
		Iterator it = connectedNodeList.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<InetSocketAddress, Boolean> entry = (Map.Entry<InetSocketAddress, Boolean>)it.next();
			if(entry.getValue().booleanValue()==false)
			{
				list.add(entry.getKey());
			}
		}
		return list;
	}

	public boolean contains(InetSocketAddress socketAddress){
		if(isEmpty())
			return false;

		if(acceptedNodeList.containsKey(socketAddress) || connectedNodeList.containsKey(socketAddress) || requestedNodeList.containsKey(socketAddress) || candidateSet.contains(socketAddress))
			return true;
		else
			return false;
	}

	//------------------------------------------------------------
	public void removeCandidate(InetSocketAddress sa){
		candidateSet.remove(sa);
	}

	public void removeRequestedNode(InetSocketAddress sa){
		requestedNodeList.remove(sa);
	}

	public void removeAcceptedNode(InetSocketAddress sa){
		acceptedNodeList.remove(sa);
	}

	public void removeConnectedNode(InetSocketAddress sa){
		connectedNodeList.remove(sa);
	}

	public boolean isEmpty(){
		if(candidateSet.isEmpty() && requestedNodeList.isEmpty() 
				&& acceptedNodeList.isEmpty() && connectedNodeList.isEmpty())
			return true;
		else
			return false;
	}
	//----------------
	public boolean isAcceptedComplete(){
		return isComplete(acceptedNodeList.values());
	}

	public boolean isConnectedComplete(){
		return isComplete(connectedNodeList.values());
	}

	boolean isComplete(Collection<Boolean> values){
		boolean complete = false;
		int count = 0;

		for(Boolean value : values){
			if(value.booleanValue()==true)
				count++;

			if(count >= ensembleSize-1){
				complete = true;
				break;
			}
		}
		return complete;
	}
	//-------------------------------



}
