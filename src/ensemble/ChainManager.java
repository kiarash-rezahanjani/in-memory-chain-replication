package ensemble;

import utility.Configuration;
import utility.NetworkUtil;
import utility.TextFile;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;

import com.google.protobuf.InvalidProtocolBufferException;

import coordination.Protocol;
import coordination.Protocol.EnsembleBean;
import coordination.Znode.EnsembleData;
import coordination.Znode.EnsembleData.Member;
import coordination.Znode.ServerData;
import coordination.Znode.ServerData.Status;
import coordination.ZookeeperClient;

import client.Log.LogEntry;
import client.Log.LogEntry.Type;

import streaming.BufferClient;
import streaming.BufferServer;


public class ChainManager implements EnsembleManager, ClientServerCallback, Watcher{

	Configuration conf;
	List<InetSocketAddress> sortedChainSocketAddress;
	public Ensemble ensemble = null;
	BufferServer server;
	BufferClient client;
	List<Channel> unknownChannels = new ArrayList<Channel>();// not necessary
	ZookeeperClient zkCli = null;
	
	//added
	String ensembleZnodePath = null;
	boolean leader = false;
	Protocol coordinator = null;
	int capacityLeft = 100;
	ZkUpdate zkUpdateThread = null;
	TextFile memoryInfo ;

	public ChainManager(Configuration conf) throws Exception{
		this.conf=conf;
		memoryInfo = new TextFile("memory report"+conf.getBufferServerPort());
		server = new BufferServer(conf, this);
		client = new BufferClient(conf, this);
		zkCli = new ZookeeperClient(this, conf);
		coordinator = new Protocol(conf, zkCli, this);
		zkUpdateThread = new ZkUpdate();
		zkUpdateThread.start();
	}

	//---------------------------------------------------------ZooKeeper----------------------------------------------------------------------

	//============================================================================================================================
	public boolean newEnsemble( EnsembleBean  ensembleBean) {
		//List<InetSocketAddress> sortedBufferServerAddress = ensembleBean.getEnsembleBufferServerAddressList();
		//for testing at the moment only one ensemble is allowed
		if(ensemble!=null)
			return false;
		//Zookeeper sets watch on the servers
		try{
			ensemble = new Ensemble(conf,ensembleBean.getEnsembleBufferServerAddressList(), ensembleBean.getEnsembleProtocolAddressList(), this);
			Channel succChannel=null;
			do{//loop is for testing, it should not happen to be null
				Thread.sleep(500);
				succChannel = client.connectServerToServer(ensemble.getSuccessorSocketAddress());
			}while(succChannel==null);

			Channel predChannel=null;
			do{//loop is for testing, it should not happen to be null
				Thread.sleep(500);
				predChannel = client.connectServerToServer(ensemble.getPredessessorSocketAddress());
			}while(predChannel==null);

			if(succChannel==null || predChannel==null)
				throw new Exception("Predecessor or successor channel is null.");
			ensemble.setSuccessor(succChannel);
			ensemble.setPredecessor(predChannel);

			//for broadcasting the persisted messages---------
			for(InetSocketAddress socketAddress : ensemble.getSortedChainSocketAddress()){
				if(NetworkUtil.isEqualAddress(socketAddress, ensemble.getPredessessorSocketAddress())
						||NetworkUtil.isEqualAddress(socketAddress, conf.getBufferServerSocketAddress()))
					continue;
				ensemble.getPeersChannelHandle().add( 
						client.connectServerToServer(socketAddress) );
			}
			ensemble.getPeersChannelHandle().add(predChannel);
			for(InetSocketAddress protocolSocketAddress : ensembleBean.getEnsembleProtocolAddressList() ){
				Stat stat = zkCli.setServerFailureDetector(protocolSocketAddress);
				if(stat==null){
					ensemble.close();
					return false;
				}
				
			}
			System.out.println("/n/nPEER CHANNEL" + ensemble.getPeersChannelHandle());
			return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
	}


	/**
	 * This method should be called to set the ensemble path only if the server is the leader of the ensemble
	 * If the path is set the server consider itself as the leader and updates the ensemble data
	 */
	public void setEnsembleZnodePath(String path, boolean isLeader){
		ensembleZnodePath = path;
		this.leader = isLeader;
	}

	public void shutDownEnsemble(){
		System.out.println("Shutdown ensemble.." + conf.getProtocolPort() + " Ensemble " + ensemble.getSortedChainSocketAddress());
		ensemble.ensembleFailed();
	}
	
	public void cleanEnsemble(){
		
		ensemble.close();
		ensemble=null;
		System.out.println("Ensemble is Shutdown. All cleaned.");
		throw new RuntimeException();//for testing 
	}
	//-------------------------------Messages From the Servers and Clients---AND--- ACTIONs Taken---------------------------------
	@Override
	public void serverReceivedMessage(MessageEvent e) {
		try{//for now we have the try catch later change to the rght exception type
			LogEntry msg = (LogEntry) e.getMessage();
			//System.out.println("Rec: "  + msg.getEntryId().getMessageId() + " " + msg.getMessageType() );
			if(msg.hasMessageType()){//check if this is a channel identification message

				if(msg.getMessageType()==Type.ENTRY_PERSISTED){
					//System.out.println("Persisted Message: " +  msg.getEntryId().getMessageId() + " Channel " +  e.getChannel());
					ensemble.entryPersisted(msg);

				}else
					if(unknownChannels.size()>0 && msg.getMessageType()==Type.CONNECTION_BUFFER_SERVER){
						if(!unknownChannels.contains(e.getChannel()))
							throw new Exception("Server received an unknown channel identification message.");
						boolean removed = unknownChannels.remove(e.getChannel());
						System.out.println("Server " + conf.getBufferServerSocketAddress()+ " Received Connection from " + msg.getClientSocketAddress()
								+ "Removed From UnknowList: " + removed);	
					}else
						if(unknownChannels.size()>0 && msg.getMessageType()==Type.CONNECTION_DB_CLIENT){
							if(!unknownChannels.contains(e.getChannel()))
								throw new Exception("Server received an unknown channel identification message.");
							boolean removed = unknownChannels.remove(e.getChannel());
							System.out.println("Head Client " + msg.getClientSocketAddress() + " added to" + conf.getBufferServerSocketAddress()+
									"Removed From UnknowList: " +	removed );
							//find the ensemble for the client: we have to add new field to proto : ensemble name
							ensemble.addHeadDBClient(msg.getEntryId().getClientId(), msg.getClientSocketAddress(), e.getChannel());
						}else
							if(msg.getMessageType()==Type.TAIL_NOTIFICATION){
								Channel tailChannel = client.connectServerToClient(NetworkUtil.parseInetSocketAddress(msg.getClientSocketAddress()));
								ensemble.addTailDBClient(msg.getEntryId().getClientId(), tailChannel);
								System.out.println("Tail CLient " + msg.getEntryId().getClientId() + " added to " + conf.getBufferServerSocketAddress());
							}else
								if(msg.getMessageType()==Type.LAST_ACK_SENT_TO_FAILED_CLIENT){
									ensemble.clientFailedLastAck(msg.getEntryId());
								}

				//replace with a switch
				if(msg.getMessageType()==Type.ACK){//should n not happen here
					System.out.println("DBClient Rec Ack: " + msg.getEntryId() + " from " + msg.getClientSocketAddress() + " BDClient address " + conf.getBufferServerPort());
				}

			}else
			{
				ensemble.addToBuffer(msg);
				//System.out.println(conf.getBufferServerSocketAddress().getPort() + " buffered " + msg.getEntryId() );
			}
		}catch(Exception ex)
		{ex.printStackTrace(); System.exit(-1);}
	}


	@Override
	public void clientReceivedMessage(MessageEvent e) {
		// TODO Auto-generated method stub
		//either remove or tail

	}

	@Override
	public void channelClosed(ChannelStateEvent e) {
		// TODO Auto-generated method stub
		closeOnFlush(e.getChannel(), "chennelClosed.Stat:" + e.getState());

	}

	@Override
	public void serverAcceptedConnection(ChannelStateEvent e) {
		// TODO Auto-generated method stub
		System.out.println("Server: " + conf.getBufferServerPort() + " |accepted channel" +e.getChannel());
		unknownChannels.add(e.getChannel());

	}

	@Override
	public void exceptionCaught(ExceptionEvent e) {
		// TODO Auto-generated method stub

		closeOnFlush(e.getChannel(), "exceptioncaught " + e.getCause().toString());
	}

	void closeOnFlush(final Channel ch, String cause) {
		if (ch.isConnected()) {
			System.out.println("\n\nClosing the channel " +  ch + " by "  );

			ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					// TODO Auto-generated method stub
					//if(future.isDone())
					if(future.isDone()){
						ensemble.channelClosed(ch);
						future.getChannel().close();
					}
				}
			});
		}
	}

	public void close(){
		ensemble.close();
		server.stop();
		client.stop();
		zkUpdateThread.stopRunning();
		memoryInfo.close();
	}

	//---------------------should be in chain manager
	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		
		if (event.getType()== Event.EventType.NodeDeleted){
			//if the failed node is a server
			System.out.println("Failed: " + path);
			if(path.contains(conf.getZkServersRoot()+"/")){
				String node = path.replace(conf.getZkNameSpace()+conf.getZkServersRoot()+"/", "");
				
				System.out.println("Chain manager. FailedNode: "+node+ " ensemble members Bs:  " + ensemble.getMembersBufferServerAddress() +  " Members Prot " + ensemble.getMembersProtocolSocketAddress() );
				
				if(ensemble.getMembersProtocolSocketAddress().contains( NetworkUtil.parseInetSocketAddress(node))){
					if(ensembleZnodePath!=null){
						System.out.println("deleting ensemble znode....");
						zkCli.deleteEnsembleZnode(ensembleZnodePath);
						ensembleZnodePath=null;
						leader = false;
					}
					/**
					 * We need  to change the status of the server so it does not accept ensemble request.
					 */
					ensemble.ensembleFailed();
					System.out.println("Chain manager. called ensembleFailed().");
				}
				coordinator.process(event);
			}
			//if the failed node is a client
			if(path.contains(conf.getZkClientRoot())){
				String node = path.replace(conf.getZkNameSpace()+conf.getZkClientRoot()+"/", "");
				ensemble.clientFailed(node);
			}
			if(path.contains(conf.getZkServersGlobalViewRoot())){
				;//CALLBACK+ENSEMBLE.globalFailure(path);
				coordinator.process(event);
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
				System.out.println("Zookeeper Connection is dead.");
				System.exit(-1);
				break;
			}
		}
	}

	/**
	 * Updates the capacity of the the server in ensemble and server node
	 * @author root
	 *
	 */
	class ZkUpdate extends Thread{
		int currentCapacityLeft;
		boolean running = true;
		public void stopRunning(){
			running = false;
			this.interrupt();
		}
		public void run(){
			running = true;
			System.out.println("ZkUpdate is running.." + conf.getProtocolPort() );
			while(running){
				try {
					Thread.sleep(new Random().nextInt(conf.getServerInfoIntervalDeviation())+conf.getServerInfoInterval());
					Runtime.getRuntime().gc();
					currentCapacityLeft = getcapacityLeft();
					//update server Znode
					if(Math.abs(currentCapacityLeft-capacityLeft) > conf.getServerUpdateThreshold()){
						zkCli.updateServerZnode(getServerData(currentCapacityLeft, currentCapacityLeft<conf.getSaturationPoint() ? Status.REJECT_ENSEMBLE_REQUEST : Status.ACCEPT_ENSEMBLE_REQUEST ));
						capacityLeft = currentCapacityLeft;
					}
					if(ensemble == null)
						coordinator.checkForLeaderShip();
					else
						if(leader){
							if(zkCli.exists(ensembleZnodePath)){
								EnsembleData ensembleData = zkCli.getEnsembleData(ensembleZnodePath);
								int minCapacity = 100;
								for( Member member : ensembleData.getMembersList() ){
									ServerData serverdata  = zkCli.getServerZnodeDataByProtocolSocketAddress(member.getSocketAddress());
									if(serverdata!=null)
										minCapacity = Math.min(minCapacity, serverdata.getCapacityLeft());
								}
								zkCli.updateEnsembleZnode(ensembleZnodePath, 
										EnsembleData.newBuilder(ensembleData).setCapacityLeft(minCapacity).setStat(
												minCapacity<conf.getSaturationPoint() ? EnsembleData.Status.REJECT_CONNECTION : EnsembleData.Status.ACCPT_CONNECTION).build());
							}
						}					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new RuntimeException();
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvalidProtocolBufferException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		//	System.out.println("ZkUpdate stopped.." + conf.getProtocolPort() );
		}

		
		private int getcapacityLeft() {
			// TODO Auto-generated method stub
		//	System.out.println(Runtime.getRuntime().freeMemory()  + "  ---  " + Runtime.getRuntime().totalMemory());
		//	return new Random().nextInt(100);
			long totalMemeory = Runtime.getRuntime().totalMemory();
			long freeMemory = Runtime.getRuntime().freeMemory();
			String str = "";
			if(ensemble!=null)
				str = "Buffer size/capacity: " +  ensemble.getBuffer().size() + "/" + conf.getEnsembleBufferSize();
			memoryInfo.print("Free / Total memory: " + freeMemory + " " + totalMemeory + str);
			return (int) ((((double)freeMemory)/totalMemeory) * 100);
		}

		ServerData getServerData(int capacity, ServerData.Status status){
			ServerData.Builder data = ServerData.newBuilder();
			data.setCapacityLeft(capacity);
			data.setSocketAddress( conf.getProtocolSocketAddress().toString());
			data.setBufferServerSocketAddress(conf.getBufferServerSocketAddress().toString());
			data.setStat(status);
			return data.build();
		}
	}




}
