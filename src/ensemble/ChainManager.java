package ensemble;

import utility.Configuration;
import utility.NetworkUtil;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

import coordination.ZookeeperClient;

import client.Log.LogEntry;
import client.Log.LogEntry.Type;

import streaming.BufferClient;
import streaming.BufferServer;

public class ChainManager implements ClientServerCallback, Watcher{

	Configuration conf;
	List<InetSocketAddress> sortedChainSocketAddress;
	public Ensemble ensemble;
	BufferServer server;
	BufferClient client;
	List<Channel> unknownChannels = new ArrayList<Channel>();// not necessary
	static ZookeeperClient zkCli = null;

	public ChainManager(Configuration conf) throws Exception{
		this.conf=conf;
		server = new BufferServer(conf, this);
		client = new BufferClient(conf, this);
		zkCli = new ZookeeperClient(this, conf);
		
	}
	
	//---------------------------------------------------------ZooKeeper----------------------------------------------------------------------

//============================================================================================================================
	public boolean newEnsemble(List<InetSocketAddress> sortedChainSocketAddress) throws Exception{
		//Zookeeper sets watch on the servers
		ensemble = new Ensemble(conf,sortedChainSocketAddress);
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
		for(InetSocketAddress socketAddress : sortedChainSocketAddress){
			if(NetworkUtil.isEqualAddress(socketAddress, ensemble.getPredessessorSocketAddress())
				||NetworkUtil.isEqualAddress(socketAddress, conf.getBufferServerSocketAddress()))
				continue;
			ensemble.getPeersChannelHandle().add( 
					client.connectServerToServer(socketAddress) );
		}
		ensemble.getPeersChannelHandle().add(predChannel);
		System.out.println("/n/nPEER CHANNEL" + ensemble.getPeersChannelHandle());
		
		//-------------------
		return true;
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
									ensemble.clientFailed(msg.getEntryId());
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

	}
	
	//---------------------should be in chain manager
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if (event.getType()== Event.EventType.NodeDeleted){
			//if the failed node is a server
			if(path.contains(conf.getZkServersRoot()))
				;//CALLBACKTO+ENSEMBLEAND+PROTOCOL.serverFailure(path);

			//if the failed node is a client
			if(path.contains(conf.getZkClientRoot()))
				;//CALLBACKTOENSEMBLE.clientFailure(path);
			
			if(path.contains(conf.getZkServersGlobalViewRoot()))
				;//CALLBACK+ENSEMBLE.globalFailure(path);
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


}
