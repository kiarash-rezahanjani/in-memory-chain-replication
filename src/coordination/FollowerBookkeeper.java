package coordination;

import java.net.InetSocketAddress;
import java.util.List;

public class FollowerBookkeeper {

	InetSocketAddress leader = null;
	List<InetSocketAddress> ensembleMembers;
	String ensemblePath = null ;

	public List<InetSocketAddress> getEnsembleMembers() {
		return ensembleMembers;
	}

	public void setEnsembleMembers(List<InetSocketAddress> ensembleMembers) {
		this.ensembleMembers = ensembleMembers;
	}

	public FollowerBookkeeper() {

	}

	public InetSocketAddress getLeader() {
		return leader;
	}

	public void setLeader(InetSocketAddress leader) {
		this.leader = leader;
	}

	public void clear()
	{
		leader = null;
		if(ensembleMembers!=null)
			ensembleMembers.clear();
		ensemblePath = null;
	}

	public boolean isEmpty()
	{
		return leader==null && ensembleMembers.isEmpty() && ensemblePath == null;
	}

	public String getEnsemblePath() {
		return ensemblePath;
	}

	public void setEnsemblePath(String ensemblePath) {
		this.ensemblePath = ensemblePath;
	}

	public boolean contains(InetSocketAddress socketAddress){
		if(isEmpty())
			return false;
		if(socketAddress.equals(leader) )
			return true;
		else 
			return false;
	}

}
