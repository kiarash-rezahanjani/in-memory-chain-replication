package coordination;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;


import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.google.protobuf.InvalidProtocolBufferException;

import protocol.FollowerBookkeeper;
import protocol.LeaderBookKeeper;
import protocol.Protocol;


import utility.Configuration;
import utility.NetworkUtil;
import utility.Znode.EnsembleData;
import utility.Znode.ServerData;
import utility.Znode.ServersGlobalView;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class InterProcessCoordinator implements Watcher{


	ZookeeperClient zkCli;
	ExecutorService executor;

	//chains
	MembershipBookkeeper mbk ;
	Protocol protocol;
	int totalLoad ;//later on replaced by an object containing cpu memory and bandwidth consumption
	ServersGlobalView serversGlobalView;
	Status status = new Status(ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST);
	Status lastCheckpointedStatus = new Status(ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST);
	Configuration config;
	static final String defaultConfigFile = "applicationProperties";
	final int SATURATION_POINT=100;
	boolean operationLeader=false;
	LeaderBookKeeper lbk = new LeaderBookKeeper();
	FollowerBookkeeper fbk = new FollowerBookkeeper();
	//InetSocketAddress currentLeader=null;

	//clients
	//...

	//persistence
	//...
	public InterProcessCoordinator()
	{
		this(defaultConfigFile);
	}

	public InterProcessCoordinator(String configFile)
	{
		try {
			//System.out.println(configFile);
			config = new Configuration(configFile);
			mbk = new MembershipBookkeeper(config);
			zkCli = new ZookeeperClient(this, config);
			zkCli.createServerZnode(getInitialServerData());

			//for testing
			if(configFile=="applicationProperties")
				protocol = new Protocol(config, this, true, lbk, fbk);
			else
				protocol = new Protocol(config, this, false, lbk, fbk);

		}catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	}

	ServerData getInitialServerData()
	{
		ServerData.Builder data = ServerData.newBuilder();

		data.setCapacityLeft(new Random().nextInt(101));
		data.setSocketAddress( config.getProtocolSocketAddress().toString());
		data.setBufferServerSocketAddress(config.getBufferServerSocketAddress().toString());
		data.setStat(ServerData.Status.ACCEPT_ENSEMBLE_REQUEST);

		return data.build();
	}

	public Status getStatusHandle() {
		return status;	
	}

	public ZookeeperClient getZkHandle() {
		return zkCli;	
	}

	public Status getLastCheckpointedStatus(){
		return lastCheckpointedStatus;
	}

	public Configuration getConfigurationHandle()
	{
		return config;
	}

	public boolean isLeader(){
		if(status.getStatus()==ServerStatus.FORMING_ENSEMBLE_LEADER_STARTED
				||status.getStatus()==ServerStatus.FORMING_ENSEMBLE_LEADER_WAIT_FOR_ACCEPT
				||status.getStatus()==ServerStatus.FORMING_ENSEMBLE_LEADER_WAIT_FOR_CONNECTED_SIGNAL
				||status.getStatus()==ServerStatus.FORMING_ENSEMBLE_LEADER_EXEC_ROLL_BACK
				||status.getStatus()==ServerStatus.FIXING_ENSEMBLE_LEADER_WAIT_FOR_ACCEPT
				||status.getStatus()==ServerStatus.FIXING_ENSEMBLE_LEADER_WAIT_FOR_CONNECTED_SIGNAL)
			return true;
		else
			return false;
	}
	//status needs to be synchronized, so we need an object to enable fine grained synchronization
	public class Status
	{
		short status = ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST; 

		public Status(short status)
		{
			this.status = status;
		}

		public short getStatus() {
			return status;	
		}	
		public void setStatus(short status) {
			this.status =  status;
		}
	}

	/**
	 * This method is invoked when the server becomes in charge of updating the global view.
	 */
	public void startGlobalViewUpdater()
	{
		Executors.newSingleThreadExecutor();
		executor.submit(new GlobalViewServer(zkCli, 3000)); 
		//maybe we should create an ephemeral node
	}

	public void stopGlobalViewUpdater()
	{
		executor.shutdownNow();
		//delete the ephemeral node
	}

	public void setClientLoad(String clientIdentifier, int load)
	{
		//....
		updateTotalLoad(clientIdentifier, load);
	}

	synchronized void  updateTotalLoad(String clientIdentifier, int load)
	{
		// -oldvalue +newvalue
	}
	//testing version
	public List<InetSocketAddress> getSortedCandidates() //throws InvalidProtocolBufferException, KeeperException, InterruptedException {
		List<ServerData> sortedServers;
		List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
		try {
			for(;;)//for testing
			{
				sortedServers = zkCli.getSortedServersList();
				if(sortedServers.size()>=3)
					break;
				Thread.sleep(1000);
			}

			list = new ArrayList<InetSocketAddress>();

			for(ServerData s : sortedServers)
			{
				if(!config.getProtocolSocketAddress().toString().contains(s.getSocketAddress()))
					list.add(NetworkUtil.parseInetSocketAddress( s.getSocketAddress()) );
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return list;
	}


	public boolean followerConnectsEnsemble(List<InetSocketAddress> ensembleMembers) 
	{
		for(InetSocketAddress socketAddress : ensembleMembers)
		{
			try {
				Stat stat = zkCli.setServerFailureDetector(socketAddress);
				//System.out.println(config.getProtocolSocketAddress()+" SET DETECTOR ON " +socketAddress);
				if(stat==null)
				{
					System.out.println("Null server node. doesnt exist!");
					return false;
					//System.exit(-1);
				}
				ServerData serverData = zkCli.getServerZnodeDataByProtocolSocketAddress(socketAddress);
			
				//	System.out.println("Ensemble Member Server Socket: "+serverData.getBufferServerSocketAddress());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(-1);
			} 
		}

		return true;
	}
	/**
	 * change: data can be set at startService time rather than here. here we can just set 0 byte[].
	 * @param ensembleMembers
	 * @return
	 */
	public String leaderCreatesEnsemble(List<InetSocketAddress> ensembleMembers)
	{
		EnsembleData.Builder ensembleData = EnsembleData.newBuilder();
		String absolutePath=null;
		int minCapacity = SATURATION_POINT;

		try {
			for(InetSocketAddress ensembleMember : ensembleMembers)
			{

				ServerData serverData;

				serverData = zkCli.getServerZnodeDataByProtocolSocketAddress(ensembleMember);

				//System.out.println("ServerData:" + serverData.getCapacityLeft());
				EnsembleData.Member.Builder member = EnsembleData.Member.newBuilder();
				member.setSocketAddress(ensembleMember.toString());
				ensembleData.addMembers(member);

				if(serverData.getCapacityLeft() < minCapacity)
					minCapacity=serverData.getCapacityLeft();
			}

			ensembleData.setCapacityLeft(minCapacity);
			ensembleData.setStat(EnsembleData.Status.REJECT_CONNECTION);
			ensembleData.setLeader(config.getProtocolSocketAddress().toString());

			absolutePath = zkCli.createEnsembleZnode(ensembleData.build());

		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block

			//although there is no need for this code but let it be there
			if(e.code() == KeeperException.Code.NONODE)
				absolutePath = null;

			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return absolutePath;
	}

	/**
	 * Retrieve ensemble data from the given Znode and add it to membership list.  
	 * @param ensemblePath
	 */
	public void followerStartsService(String ensemblePath)
	{
		EnsembleData ensembleData;
		try {
			ensembleData = zkCli.getEnsembleData(ensemblePath);
			mbk.put(ensemblePath, ensembleData);
			//	updateServerZnodeEnsembles(ensemblePath, 1);
			//System.out.print("Folowers READ : " + config.getProtocolPort() + " Cap: " + ensembleData.getCapacityLeft()
			//		+ " Leader: " + ensembleData.getLeader()+ " Members: " + ensembleData.getMembersList() + " stat: " + ensembleData.getStat() );

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		} 

	}
	/**
	 * Add or remove ensemble from Server zonde ensemble list.
	 * @param ensemblePath
	 * @param operation:  negative number removes, 0 or positive number adds
	 */
	public void updateServerZnodeEnsembles(String ensemblePath, int operation)
	{
		ServerData serverData;
		try {
			serverData = zkCli.getServerZnodeDataByProtocolSocketAddress(config.getProtocolSocketAddress());
			List<String> ensembleList = new ArrayList<String>( serverData.getEnsembleListList() );
			if(operation < 0)
				ensembleList.remove(ensemblePath);
			else
				ensembleList.add(ensemblePath);

			zkCli.updateServerZnode(serverData.newBuilder().clearEnsembleList().addAllEnsembleList(ensembleList).build() );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}


	}

	/**
	 * Update the ensemble status so that client would be able to connect to the ensemble. 
	 * @param ensemblePath
	 */
	public void leaderStartsService(String ensemblePath)
	{
		EnsembleData ensembleData;
		try {

			ensembleData = zkCli.getEnsembleData(ensemblePath);
			ensembleData = ensembleData.newBuilder(ensembleData).setStat(EnsembleData.Status.ACCPT_CONNECTION).build();
			zkCli.updateEnsembleZnode(ensemblePath, ensembleData);
			mbk.put(ensemblePath, ensembleData);
		//	System.out.print("LEADER UPDATES: " + config.getProtocolPort() + " Cap: " + ensembleData.getCapacityLeft()
		//			+ " Leader: " + ensembleData.getLeader()+ " Members: " + ensembleData.getMembersList() + " stat: " + ensembleData.getStat() );
			//updateServerZnodeEnsembles(ensemblePath, 1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		} 
	}

	//--------------------------
	//@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	@Override
	public void process(WatchedEvent event) {

		String path = event.getPath();

		if (event.getType()== Event.EventType.NodeDeleted)
		{
			//if the failed node is a server
			if(path.contains(config.getZkServersRoot()))
				serverFailure(path);

			//if the failed node is a client
			if(path.contains(config.getZkClientRoot()))
				clientFailure(path);
		}

		if (event.getType() == Event.EventType.None) 
		{
			// We are are being told that the state of the
			// connection has changed
			switch (event.getState()) {
			case SyncConnected:
				// In this particular example we don't need to do anything
				// here - watches are automatically re-registered with 
				// server and any watches triggered while the client was 
				// disconnected will be delivered (in order of course)
				break;
			case Expired:
				// It's all over
				System.out.println("Zookeeper Connection is dead");
				System.exit(-1);
				break;
			}
		}
	}

	private void clientFailure(String path) {
		System.out.println("ServerDead:"+path);

	}

	String getNameFromPath(String path){
		if(!path.contains("/"))
			return path;

		return getNameFromPath(path.substring( path.indexOf("/")+1 ) );
	}

	private void serverFailure(String path) {
		System.out.println("ClientDead1:"+path);
		String nodeName = getNameFromPath(path);

		/**
		 * If I am the leader and on of the follower of the current operation has failed
		 * then perform rollBack.
		 * If I am the follower rollBack only if the leader of the operation has failed
		 * otherwise do nothing and follow the leader command.
		 */
/*		System.out.println("FFFFFFFFFFFFUCK      " + lbk.getClass().toString());
		System.out.println("FFFFFFFFFFFFUCK\n"+

				!lbk.isEmpty() 
				+"\n"+ !lbk.getConnectedList().isEmpty()
				+"\n"+  NetworkUtil.contains( lbk.getConnectedList(),nodeName)		
				+"\n"+NetworkUtil.contains( lbk.getNotConnectedList(),nodeName)		
				+"\n"+ NetworkUtil.contains( lbk.getNotConnectedList(),nodeName)
				+"\n"+NetworkUtil.contains( lbk.getAcceptedList(),nodeName)		
				+"\n"+NetworkUtil.contains( lbk.getNotAcceptedList(),nodeName)		
				+"\n"+	fbk.isEmpty()	
				);
				*/
		//protocol.rollBack("serverFailure: "+ path + " called by " + config.getProtocolSocketAddress());
	
		if( !protocol.getLeaderBookKeeperHandle().isEmpty() && 
				!protocol.getLeaderBookKeeperHandle().getConnectedList().isEmpty() ){
			if( NetworkUtil.contains( protocol.getLeaderBookKeeperHandle().getConnectedList(),nodeName)){
				protocol.rollBack("serverFailure");
				return;
			}else{
				if(NetworkUtil.contains( protocol.getLeaderBookKeeperHandle().getNotConnectedList(),nodeName))
					return;
			}
		}else
			if( !protocol.getLeaderBookKeeperHandle().isEmpty()  ){
				if(NetworkUtil.contains( protocol.getLeaderBookKeeperHandle().getAcceptedList(),nodeName))
					protocol.rollBack("serverFailure");
				else
					if(NetworkUtil.contains( protocol.getLeaderBookKeeperHandle().getNotAcceptedList(),nodeName))
						return;
			}
			else
				if( !protocol.getFollowerBookkeeperHandle().isEmpty() && 
						NetworkUtil.isEqualAddress( protocol.getFollowerBookkeeperHandle().getLeader(),nodeName) ){
					protocol.rollBack("serverFailure");
				}else
				{
					/**
					 * check if the server is one of the current active servers
					 * if so, change to broken chain mode.
					 */
				}

		/*
		if(isLeader()){
			System.out.println("leader status befo roll"+	status.getStatus());
			protocol.rollBack();
			System.out.println("leader status aftter roll"+	status.getStatus());
		}
		 */	//if(isLeader() && protocol.getLeaderBookKeeperHandle().getAcceptedList().toString().contains(path))

		//if from current forming ensemble then roll back operation
		//if not see which ensemble it is 
		//start logging synchronously for that ensemble
		//see who is the leader if dead figure out who is new
		//if I am the leader start the operation 
		//if not wait for leader command
		//

	}



}
