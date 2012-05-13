package coordination;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;

import com.google.protobuf.InvalidProtocolBufferException;

import coordination.rpc.SenderReceiver;
import utility.Configuration;
import utility.NetworkUtil;
import coordination.Znode.EnsembleData;
import coordination.Znode.ServerData;
import coordination.Znode.ServersGlobalView;

public class Protocol implements Runnable, ReceivedMessageCallBack, Watcher/*for testing*/{
	private SenderReceiver senderReceiver;
	//ExecutorService executor;
	Configuration conf;
	LeaderBookKeeper lbk = new LeaderBookKeeper();
	FollowerBookkeeper fbk = new FollowerBookkeeper();
	ZookeeperClient zkCli;
	AtomicInteger status = new AtomicInteger(ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST);
	AtomicInteger checkPointedStatus = new AtomicInteger();
	boolean running = true;
	ServersGlobalView serversGlobalView;
	GlobalViewServer gvServer;
	boolean globalViewUpdater = false;

	public Protocol(Configuration conf, ZookeeperClient zkCli ) {
		this.conf = conf;
		this.zkCli = zkCli;
		senderReceiver = new SenderReceiver(conf, this);

		try {
			zkCli.createServerZnode(getInitialServerData());
			//executor = Executors.newFixedThreadPool(2);
			gvServer = new GlobalViewServer(zkCli, 8000);
			startGlobalViewServer();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	ServerData getInitialServerData(){
		ServerData.Builder data = ServerData.newBuilder();
		data.setCapacityLeft(new Random().nextInt(101));
		data.setSocketAddress( conf.getProtocolSocketAddress().toString());
		data.setBufferServerSocketAddress(conf.getBufferServerSocketAddress().toString());
		data.setStat(ServerData.Status.ACCEPT_ENSEMBLE_REQUEST);
		return data.build();
	}

	//for testing only
	boolean leader=false;

	public Protocol(Configuration conf, boolean leader){
		this.conf = conf;
		senderReceiver = new SenderReceiver(conf, this);
		try{
			this.zkCli = new ZookeeperClient(this, conf);
			zkCli.createServerZnode(getInitialServerData());
			//executor = Executors.newFixedThreadPool(2);
			gvServer = new GlobalViewServer(zkCli, 8000);
			startGlobalViewServer();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.leader = leader;
		if(leader)
			leaderStartsFormingEnsemble(3);

	}

	public void setStatus(int expect, int update){
		this.status.compareAndSet(expect, update);
	}

	@Override 
	public void received(Object msg, InetSocketAddress srcSocketAddress) {

		ProtocolMessage message = (ProtocolMessage) msg;
		System.out.println("From: " + message.getSrcSocketAddress().getPort() + " To: " + senderReceiver.getServerSocketAddress().getPort() 
				+ " MsgType: " + message.getMessageType());

		switch(status.get()){
		case ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST:
			if(message.getMessageType()== MessageType.ACCEPTED_JOIN_ENSEMBLE_REQUEST)
				abortOperation(message.getSrcSocketAddress());

			addStat();
			followerAcceptRequest(message);
			addStat();
			break;
			//======================================================================================
		case ServerStatus.FORMING_ENSEMBLE_LEADER_STARTED: 
			if(message.getMessageType()== MessageType.ACCEPTED_JOIN_ENSEMBLE_REQUEST)
				abortOperation(message.getSrcSocketAddress());
			break;
			//======================================================================================
		case ServerStatus.FORMING_ENSEMBLE_LEADER_WAIT_FOR_ACCEPT: 
			addStat();
			leaderProcessWaitForAccept(message);
			addStat();
			break;
			//======================================================================================
		case ServerStatus.FORMING_ENSEMBLE_LEADER_WAIT_FOR_CONNECTED_SIGNAL: 
			if(message.getMessageType()== MessageType.ACCEPTED_JOIN_ENSEMBLE_REQUEST)
				abortOperation(message.getSrcSocketAddress());
			addStat();
			leaderWaitForConnectedSignal(message);
			addStat();
			//printStattransition();
			break;
			//======================================================================================
		case ServerStatus.FORMING_ENSEMBLE_LEADER_EXEC_ROLL_BACK: 
			if(message.getMessageType()==MessageType.OPERATION_FAILED )
				;
			break;
			//======================================================================================
		case ServerStatus.FORMING_ENSEMBLE_NOT_LEADER_STARTED: 
			addStat();
			followerStartConnections(message);
			addStat();
			break;
			//======================================================================================
		case ServerStatus.FORMING_ENSEMBLE_NOT_LEADER_CONNECTING: 
			if(message.getMessageType()==MessageType.OPERATION_FAILED )
				;
			break;
			//======================================================================================
		case ServerStatus.FORMING_ENSEMBLE_NOT_LEADER_WAIT_FOR_START_SIGNAL:
			addStat();
			followerWaitForStartService(message);
			addStat();
			//printStattransition();
			break;		
		case ServerStatus.BROKEN_ENSEMBLE: 

			break; 
		default: System.out.println("WHAT THE HELLL...."); System.exit(-1);
		}
	}


	//------------------------------------------LEADER----------------------------------------
	/**
	 * Get the list of servers available for forming ensemble, set watch on the servers and send join resuests to them.
	 * @param replicationFactor

	void leaderStartsFormingEnsemble(int replicationFactor){
		if(status.get()!=ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST  
				&& status.get()!=ServerStatus.FORMING_ENSEMBLE_LEADER_STARTED ){
			System.out.println("Formin ensemble while status.getStatus()!=ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST . ");
			System.exit(-1);
		}

		if(!lbk.isEmpty()){
			System.out.println("An attemp is made to form ensemble and Leaderbook Keeper is not empty. ");
			System.exit(-1);
		}
		List<InetSocketAddress> candidates = getSortedCandidates();//get candidates
		if(candidates.size() < replicationFactor-1){
			System.out.println("candidates.size() < replicationFactor");
			System.exit(-1);
		}
		lbk.setEnsembleSize(replicationFactor);
		lbk.addCandidateList(candidates);
		addStat();
		checkPointedStatus.set(status.get());
		status.set(ServerStatus.FORMING_ENSEMBLE_LEADER_WAIT_FOR_ACCEPT);
		addStat();
		//-1 : leader is also part of the chain
		for(int i=0 ; i<replicationFactor-1; i++){
			Stat stat = zkCli.setServerFailureDetector(candidates.get(i));
			if(stat != null){
				lbk.putRequestedNode(candidates.get(i), false);
				joinRequest(candidates.get(i));
			}
		}

		if(lbk.getRequestedNodeList().size() < lbk.getEnsembleSize()-1)
			rollBack("leaderStartsFormingEnsemble  lbk.getRequestedNodeList().size() < lbk.getEnsembleSize()-1");
	}
	 */
	void leaderStartsFormingEnsemble(int replicationFactor){
		if(status.get()!=ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST  
				&& status.get()!=ServerStatus.FORMING_ENSEMBLE_LEADER_STARTED ){
			System.out.println("Formin ensemble while status.getStatus()!=ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST . ");
			System.exit(-1);
		}

		if(!lbk.isEmpty()){
			System.out.println("An attemp is made to form ensemble and Leaderbook Keeper is not empty. ");
			System.exit(-1);
		}

		List<ServerData> candidatesDataServer = getCandidates();//get candidates
		if(candidatesDataServer.size() < replicationFactor-1){
			System.out.println("candidates.size() < replicationFactor");
			System.exit(-1);
		}
		lbk.setEnsembleSize(replicationFactor);
		lbk.setCandidatesServerData(candidatesDataServer);
		List<InetSocketAddress> candidates =  lbk.getCandidatesProtocolAddress();
		//		lbk.addCandidateList(candidates);
		addStat();
		checkPointedStatus.set(status.get());
		status.set(ServerStatus.FORMING_ENSEMBLE_LEADER_WAIT_FOR_ACCEPT);
		addStat();
		//-1 : leader is also part of the chain
		for(int i=0 ; i<replicationFactor-1; i++){
			Stat stat = zkCli.setServerFailureDetector(candidates.get(i));
			if(stat != null){
				lbk.putRequestedNode(candidates.get(i), false);
				joinRequest(candidates.get(i));
			}
		}

		if(lbk.getRequestedNodeList().size() < lbk.getEnsembleSize()-1)
			rollBack("leaderStartsFormingEnsemble  lbk.getRequestedNodeList().size() < lbk.getEnsembleSize()-1");
	}

	void leaderProcessWaitForAccept(ProtocolMessage message){
		if(message.getMessageType()== MessageType.ACCEPTED_JOIN_ENSEMBLE_REQUEST){
			lbk.putAcceptedNode(message.getSrcSocketAddress(), true);
			//sufficient number of accept messages has been received
			if(lbk.isAcceptedComplete()){
				status.set(ServerStatus.FORMING_ENSEMBLE_LEADER_WAIT_FOR_CONNECTED_SIGNAL);// need to check if there is enough capacity left
				List<InetSocketAddress> protocolAddressList = new ArrayList<InetSocketAddress>();
				List<InetSocketAddress> bufferServerAddressList = new ArrayList<InetSocketAddress>();
				protocolAddressList.add(0, conf.getProtocolSocketAddress());
				protocolAddressList.addAll(lbk.getAcceptedList());

				bufferServerAddressList.add(0, conf.getBufferServerSocketAddress());
				for(InetSocketAddress prot : lbk.getAcceptedList()){
					bufferServerAddressList.add(lbk.protocolToBufferServer.get(prot));
				}

				EnsembleBean ensembleBean = new EnsembleBean();
				ensembleBean.setEnsembleBufferServerAddressList(bufferServerAddressList);
				ensembleBean.setEnsembleProtocolAddressList(protocolAddressList);
				lbk.setEnsembleMembers(protocolAddressList );
				for(InetSocketAddress sa : lbk.getAcceptedList())
					connectSignal(sa, ensembleBean);
			}
		}

		if(message.getMessageType()== MessageType.REJECTED_JOIN_ENSEMBLE_REQUEST){
			lbk.putAcceptedNode(message.getSrcSocketAddress(), false);
			if(!lbk.waitForNextAcceptedMessage())
				rollBack("leaderProcessWaitForAccept !lbk.waitForNextAcceptedMessage()");
		}

	}
	public static class EnsembleBean implements java.io.Serializable{
		private static final long serialVersionUID = 1L;
		List<InetSocketAddress> ensembleProtocolAddresses;
		List<InetSocketAddress> ensembleBufferServerAddresses;
		public EnsembleBean(){
			ensembleProtocolAddresses = new ArrayList<InetSocketAddress>();
			ensembleBufferServerAddresses  = new ArrayList<InetSocketAddress>();
		}
		public void setEnsembleProtocolAddressList(List<InetSocketAddress> protocolAddressList){
			this.ensembleProtocolAddresses.addAll(protocolAddressList);
		}
		public void setEnsembleBufferServerAddressList(List<InetSocketAddress> bufferServerAddressList){
			this.ensembleBufferServerAddresses.addAll(bufferServerAddressList);
		}
		public List<InetSocketAddress> getEnsembleProtocolAddressList(){
			return ensembleProtocolAddresses;
		}
		public List<InetSocketAddress> getEnsembleBufferServerAddressList(){
			return ensembleBufferServerAddresses;
		}
	}
	void leaderWaitForConnectedSignal(ProtocolMessage message){
		if(message.getMessageType()== MessageType.SUCEEDED_ENSEMBLE_CONNECTION){
			lbk.putConnectedNode(message.getSrcSocketAddress(), true);

			if(lbk.isConnectedComplete()){
				addStat();
				status.set(ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST);
				addStat();
				printStattransition();
				String ensemblePath = leaderCreatesEnsembleZnode(lbk.getEnsembleMembers());
				if(ensemblePath==null){
					rollBack("leaderWaitForConnectedSignal ensmble node creation failed. Ensemble Path:"+ ensemblePath);
					return;
				}
				for(InetSocketAddress sa : lbk.getConnectedList()){
					startServiceSignal(sa, ensemblePath);
					//System.out.println("sending start sig to:"+ sa.toString());
				}
				System.out.println("CALLBACK.leaderStartsService(ensemblePath);");
				//CALLBACK.leaderStartsService(ensemblePath);//maybe its not necessary
			}
		}

		if(message.getMessageType()== MessageType.FAILED_ENSEMBLE_CONNECTION){
			lbk.putConnectedNode(message.getSrcSocketAddress(), false);
			if(!lbk.waitForNextConnectedMessage())
				rollBack("leaderWaitForConnectedSignal !lbk.waitForNextConnectedMessage()");
		}
	}

	//-------------------------------------------Follower----------------------------------	
	private void followerAcceptRequest(ProtocolMessage message) 
	{
		try {//testing
			Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(message.getMessageType()==MessageType.JOIN_ENSEMBLE_REQUEST ){
			//check pointing the current status
			checkPointedStatus.set(status.get());
			status.set(ServerStatus.FORMING_ENSEMBLE_NOT_LEADER_STARTED);
			fbk.setLeader(message.getSrcSocketAddress());
			//set failure detector on leader
			Stat stat = zkCli.setServerFailureDetector(fbk.getLeader());
			//	System.out.println(senderReceiver.getServerSocketAddress()+" SET DETECTOR ON 2 " + fbk.getLeader());
			if(stat==null){
				rollBack("followerAcceptRequest leadreDEATH");
				return;
			}
			acceptJoinRequest(message.getSrcSocketAddress());
		}

		if(message.getMessageType()==MessageType.OPERATION_FAILED )
			rollBack("followerAcceptRequest OPERATION_FAILED");

	}

	/**
	 * Here we should callback to create an ensemble.
	 * @param message
	 * @param status
	 */
	void followerStartConnections(ProtocolMessage message){
		if(message.getMessageType()==MessageType.START_ENSEMBLE_CONNECTION ){
			status.set(ServerStatus.FORMING_ENSEMBLE_NOT_LEADER_CONNECTING);
			addStat();
			EnsembleBean ensemble = (EnsembleBean) message.msgContent;	
			List<InetSocketAddress> ensembleMembersBufferServerAddress = ensemble.getEnsembleBufferServerAddressList();	
			boolean success = true;//callback //cdrHandle.followerConnectsEnsemble(ensembleMembers);
			System.out.println("BufferServers  " + ensembleMembersBufferServerAddress + " me "+ conf.getProtocolPort());
			if(success){	
				fbk.setEnsembleMembers(ensemble.getEnsembleProtocolAddressList());	
				followerConnectedSignal(message.getSrcSocketAddress());
				status.set(ServerStatus.FORMING_ENSEMBLE_NOT_LEADER_WAIT_FOR_START_SIGNAL);// need to check if there is enough capacity left
			}else{
				followerFailedConnectedSignal(message.getSrcSocketAddress());
				rollBack("followerWaitForStartService    failed = cdrHandle.followerConnectsEnsemble(ensembleMembers");
				return;
			}
		}
		if(message.getMessageType()==MessageType.OPERATION_FAILED )
			rollBack("followerStartConnections OPERATION_FAILED");
	}

	void followerWaitForStartService(ProtocolMessage message){
		if(message.getMessageType()==MessageType.START_SERVICE){
			String ensemblePath = message.getMsgContent().toString();
			if(ensemblePath==null || ensemblePath.length()==0){
				System.out.println("followerWaitForStartService() ensemblepath null!");
				System.exit(-1);
			}
			fbk.setEnsemblePath(ensemblePath);
			System.out.println("CALLBACK.followerStartsService(ensemblePath); Path: " + ensemblePath);
			//CALLBACK.followerStartsService(ensemblePath);// we dont need this as they are alrady ready for the service
			status.set(ServerStatus.ALL_FUNCTIONAL_ACCEPT_REQUEST);// need to check if there is enough capacity left
			//signal the coordinator
			addStat();
			printStattransition();
		}
		if(message.getMessageType()==MessageType.OPERATION_FAILED )
			rollBack("followerWaitForStartService OPERATION_FAILED");
	}


	//-----------------------------------------------------------------------------------------	
	//for testing
	//senderReceiver.send(srcSocketAddress, new ProtocolMessage(MessageType.ACCEPTED_JOIN_ENSEMBLE_REQUEST));
	public void joinRequest(InetSocketAddress srcSocketAddress){
		senderReceiver.send(srcSocketAddress, new ProtocolMessage(MessageType.JOIN_ENSEMBLE_REQUEST, " "));
	}

	public void acceptJoinRequest(InetSocketAddress srcSocketAddress){
		senderReceiver.send(srcSocketAddress, new ProtocolMessage(MessageType.ACCEPTED_JOIN_ENSEMBLE_REQUEST, " "));
	}

	/**
	 * Signals a follower to start establishing the connections and creating required data structures for start an ensemble with given list of servers.
	 * @param srcSocketAddress
	 * @param list of servers in ensemble. the first element of the list is the leader of ensemble.
	 */
	public void connectSignal(InetSocketAddress srcSocketAddress, Object ensemble){
		senderReceiver.send(srcSocketAddress, new ProtocolMessage(MessageType.START_ENSEMBLE_CONNECTION, ensemble));
	}

	public void followerConnectedSignal(InetSocketAddress srcSocketAddress){
		senderReceiver.send(srcSocketAddress, new ProtocolMessage(MessageType.SUCEEDED_ENSEMBLE_CONNECTION,  " "));
	}

	public void followerFailedConnectedSignal(InetSocketAddress srcSocketAddress){
		senderReceiver.send(srcSocketAddress, new ProtocolMessage(MessageType.FAILED_ENSEMBLE_CONNECTION,  " "));
	}

	public void startServiceSignal(InetSocketAddress srcSocketAddress, String ensemblePath){
		senderReceiver.send(srcSocketAddress, new ProtocolMessage(MessageType.START_SERVICE, ensemblePath));
	}

	public void abortOperation(InetSocketAddress srcSocketAddress){
		senderReceiver.send(srcSocketAddress, new ProtocolMessage(MessageType.OPERATION_FAILED,  " "));
	}

	//for testing
	List<Integer> statTransition = new ArrayList<Integer>();
	void addStat(){
		statTransition.add(status.get());
	}
	//For testing
	void printStattransition(){
		System.out.println("Me:" + senderReceiver.getServerSocketAddress()+ " Leader:" + leader + " Status:" + statTransition );
	}

	/**
	 * 	Clear follower or leader bookkeeper object and set the status to the last check pointed status before starting the operation
	 */
	public void rollBack(String message){
		System.out.println("Attemp to rollback : " + message + " MYServer:" + conf.getProtocolSocketAddress() );
		addStat();
		//if I am the leader send abort operation to all the followers
		if(isLeader()==true){
			for(InetSocketAddress follower : lbk.getRequestedNodeList())
				abortOperation(follower);
		}
		//recover the latest status before start of operation 
		status.set(checkPointedStatus.get());
		addStat();
		printStattransition();
		lbk.clear();
		fbk.clear();
	}


	public interface ServerStatus{
		public static final int FORMING_ENSEMBLE_LEADER_STARTED = 10; //request others to join the ensemble
		//int FORMING_ENSEMBLE_LEADER_SENDING_REQUEST = 33;
		int FORMING_ENSEMBLE_LEADER_WAIT_FOR_ACCEPT = 11;//wait for all approvals
		int FORMING_ENSEMBLE_LEADER_WAIT_FOR_CONNECTED_SIGNAL = 12;//wait for all to connect to each other
		int FORMING_ENSEMBLE_LEADER_EXEC_ROLL_BACK = 32;//

		int FORMING_ENSEMBLE_NOT_LEADER_STARTED = 13; //already sent accept message and wait for connecting
		int FORMING_ENSEMBLE_NOT_LEADER_CONNECTING = 14; //being requested to join an ensemble
		int FORMING_ENSEMBLE_NOT_LEADER_WAIT_FOR_START_SIGNAL =15;
		int FORMING_ENSEMBLE_NOT_LEADER_ROLL_BACK_ALL_OPERATIONS = 31;//later: when leader fails in the middle or it cancels the job

		int BROKEN_ENSEMBLE = 16; //one of the ensemble that I am member of of is broken
		int BROKEN_ENSEMBLE_FINDING_REPLACEMENT = 17; 
		int FIXING_ENSEMBLE_LEADER_WAIT_FOR_ACCEPT = 18; 
		int FIXING_ENSEMBLE_LEADER_WAIT_FOR_CONNECTED_SIGNAL = 19;
		int FIXING_ENSEMBLE_NOT_LEADER_CONNECTING = 20;//the broken ensemble is being fixed and I a listening for results from the leader	
		int FIXING_ENSEMBLE_NOT_LEADER_WAIT_FOR_START_SIGNAL = 21;

		//later to be completed
		int I_AM_LEAVING_ENSEMBLE = 22;//I leave ensemble and replace myself with another node
		int A_MEMBER_LEAVING_ENSEMBLE = 23;//a member is leaving ensemble wait for new one

		int ALL_FUNCTIONAL_REJECT_REQUEST = 24;	//all fine just server is saturated
		int ALL_FUNCTIONAL_ACCEPT_REQUEST  = 1;//( short) ServerData.Status.ACCEPT_ENSEMBLE_REQUEST.getNumber();// all fine and I can also join a new ensemble
	}

	public boolean isLeader(){
		if(status.get()==ServerStatus.FORMING_ENSEMBLE_LEADER_STARTED
				||status.get()==ServerStatus.FORMING_ENSEMBLE_LEADER_WAIT_FOR_ACCEPT
				||status.get()==ServerStatus.FORMING_ENSEMBLE_LEADER_WAIT_FOR_CONNECTED_SIGNAL
				||status.get()==ServerStatus.FORMING_ENSEMBLE_LEADER_EXEC_ROLL_BACK
				||status.get()==ServerStatus.FIXING_ENSEMBLE_LEADER_WAIT_FOR_ACCEPT
				||status.get()==ServerStatus.FIXING_ENSEMBLE_LEADER_WAIT_FOR_CONNECTED_SIGNAL)
			return true;
		else
			return false;
	}

	/**
	 * FOR TESTING change: data can be set at startService time rather than here. here we can just set 0 byte[].
	 * @param ensembleMembers
	 * @return
	 */
	public String leaderCreatesEnsembleZnode(List<InetSocketAddress> ensembleMembers){
		int SATURATION_POINT = 100;
		EnsembleData.Builder ensembleData = EnsembleData.newBuilder();
		String absolutePath=null;
		int minCapacity = SATURATION_POINT;

		try {
			for(InetSocketAddress ensembleMember : ensembleMembers){
				ServerData serverData;
				serverData = zkCli.getServerZnodeDataByProtocolSocketAddress(ensembleMember);
				EnsembleData.Member.Builder member = EnsembleData.Member.newBuilder();
				member.setSocketAddress(ensembleMember.toString());
				ensembleData.addMembers(member);

				if(serverData.getCapacityLeft() < minCapacity)
					minCapacity=serverData.getCapacityLeft();
			}
			ensembleData.setCapacityLeft(minCapacity);
			ensembleData.setStat(EnsembleData.Status.REJECT_CONNECTION);
			ensembleData.setLeader(conf.getProtocolSocketAddress().toString());
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
	//---------------------------------------------
	//testing version
	public List<InetSocketAddress> getSortedCandidates(){ //throws InvalidProtocolBufferException, KeeperException, InterruptedException 
		List<ServerData> sortedServers;
		List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
		try {
			for(;;){//for testing
				sortedServers = zkCli.getSortedServersList();
				if(sortedServers.size()>=3)
					break;
				Thread.sleep(1000);
			}
			list = new ArrayList<InetSocketAddress>();
			for(ServerData s : sortedServers){
				if(!conf.getProtocolSocketAddress().toString().contains(s.getSocketAddress()))
					list.add(NetworkUtil.parseInetSocketAddress( s.getSocketAddress()) );
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return list;
	}

	//---------------------------------------------
	//testing version
	public List<ServerData> getCandidates(){ //throws InvalidProtocolBufferException, KeeperException, InterruptedException 
		List<ServerData> sortedServers;
		List<ServerData> list = new ArrayList<ServerData>();
		try {
			for(;;){//for testing
				sortedServers = zkCli.getSortedServersList();
				if(sortedServers.size()>=3)
					break;
				Thread.sleep(1000);
			}
			list = new ArrayList<ServerData>();
			for(ServerData s : sortedServers){
				if(!conf.getProtocolSocketAddress().toString().contains(s.getSocketAddress()))
					list.add(s);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return list;
	}

	//------------------------------------------------------
	public void failureOf(String strSocketAddress){
		System.out.println("Server Failure." + strSocketAddress);
		if(isLeader()){
			if(lbk.contains(NetworkUtil.parseInetSocketAddress(strSocketAddress)))
				rollBack("one of the contacted server was failed during operation");
		}else{
			if(fbk.contains(NetworkUtil.parseInetSocketAddress(strSocketAddress)))
				rollBack("leader of operation failed during operation.");
		}
	}
	//------------------------------------------------------
	public void stopRunning(){
		running = false;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(running){

		}

	}
	//---------------------------------Global View Updater--------------------------
	public void startGlobalViewServer(){
		System.out.println("Starting Global View Server ..." + conf.getProtocolPort());
		globalViewUpdater = zkCli.createGlobalViewZnode();
		if(globalViewUpdater){
			new Thread(gvServer).start(); 
			System.out.println("Started global viewer by " + conf.getProtocolPort());
		}
		zkCli.setGlobalviewUpdaterFailureDetector();
	}
	public void stopGlobalViewUpdater(){
		gvServer.stopRunning();
		//executor.shutdownNow();
		System.out.println("Shutdown global viewer by " + conf.getProtocolPort());
	}

	//for testing
	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();

		if (event.getType()== Event.EventType.NodeDeleted){
			System.out.println("failed1."+path  + "received by " + conf.getProtocolPort());
			//if the failed node is a server
			if(path.contains(conf.getZkServersRoot()+"/"))
				failureOf(path.replace(conf.getZkEnsemblesRoot()+conf.getZkServersRoot()+"/", ""));//serverFailure(path);

			//if the failed node is a client
			if(path.contains(conf.getZkClientRoot()+"/"))
				;//clientFailure(path);
			//if the failed node is a client
			if(path.contains(conf.getZkServersGlobalViewRoot())){
				if(globalViewUpdater){
					globalViewUpdater = false;
					stopGlobalViewUpdater();
				}
				startGlobalViewServer();//clientFailure(path);
			}

		}

		if (event.getType() == Event.EventType.None) {
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

}
