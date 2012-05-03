package ensemble;

import utility.Configuration;
import utility.NetworkUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;

import client.Log.LogEntry;
import client.Log.LogEntry.Type;

import streaming.BufferClient;
import streaming.BufferServer;

public class ChainManager implements ClientServerCallback{

	Configuration conf;
	List<InetSocketAddress> sortedChainSocketAddress;
	public Ensemble ensemble;
	BufferServer server;
	BufferClient client;
	List<Channel> unknownChannels = new ArrayList<Channel>();

	public ChainManager(Configuration conf) throws Exception{
		this.conf=conf;
		server = new BufferServer(conf, this);
		client = new BufferClient(conf, this);
	}

	public boolean newEnsemble(List<InetSocketAddress> sortedChainSocketAddress) throws Exception{
		ensemble = new Ensemble(conf,sortedChainSocketAddress);
		Channel succChannel=null;
		do{//loop is for testing, it should not happen to be null
			Thread.sleep(1000);
			succChannel = client.connectServerToServer(ensemble.getSuccessorSocketAddress());
		}while(succChannel==null);
		
		Channel predChannel=null;
		do{//loop is for testing, it should not happen to be null
			Thread.sleep(1000);
			predChannel = client.connectServerToServer(ensemble.getPredessessorSocketAddress());
		}while(predChannel==null);
		
		if(succChannel==null || predChannel==null)
			throw new Exception("Predecessor or successor channel is null.");
		ensemble.setSuccessor(succChannel);
		ensemble.setPredecessor(predChannel);
		return true;
	}

	@Override
	public void serverReceivedMessage(MessageEvent e) {
		// TODO Auto-generated method stub

		try{//for now we have the try catch later change to the rght exception type
			LogEntry msg = (LogEntry) e.getMessage();
			if(msg.hasMessageType()){//check if this is a channel identification message
				//replace with a switch
				if(msg.getMessageType()==Type.ACK){//should n not happen here
					System.out.println("DBClient Rec Ack: " + msg.getEntryId() + " from " + msg.getClientSocketAddress() + " BDClient address " + conf.getBufferServerPort());
				}

				if(msg.getMessageType()==Type.ENTRY_PERSISTED){
					ensemble.entryPersisted(msg);
					System.out.println("Persisted Message Received by: " + conf.getBufferServerSocketAddress() + " MsgId " + msg.getEntryId() );
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

	static void closeOnFlush(Channel ch, String cause) {
		if (ch.isConnected()) {
			System.out.println("\n\nClosing the channel " +  ch + " by "  );
			ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
		}
	}

	public void close(){
		ensemble.close();
		server.stop();
		client.stop();
		
	}

	
}
