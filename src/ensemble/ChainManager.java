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
		//this.sortedChainSocketAddress = sortedChainSocketAddress;
		server = new BufferServer(conf, this);
		client = new BufferClient(conf, this);
	}

	public boolean newEnsemble(List<InetSocketAddress> sortedChainSocketAddress) throws Exception{
		ensemble = new Ensemble(conf,sortedChainSocketAddress);
		//	System.out.print(" Pre:" + ensemble.getPredessessorSocketAddress() );
		//	System.out.print("suscc "+ensemble.getSuccessorSocketAddress() );

		Channel succChannel = client.connectServerToServer(ensemble.getSuccessorSocketAddress());
		Channel predChannel = client.connectServerToServer(ensemble.getPredessessorSocketAddress());
		if(succChannel==null || predChannel==null)
			return false;
		//System.out.println("ESTA NULLLLL");
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
					System.out.println("Client Rec Ack: " + msg.getEntryId() + " " + conf.getBufferServerSocketAddress());
				}

				if(msg.getMessageType()==Type.ENTRY_PERSISTED){
					ensemble.entryPersisted(msg);
					System.out.println("Server Rec Persisted: " + msg.getEntryId() + "Server: " + conf.getBufferServerSocketAddress());
				}else
					if(unknownChannels.size()>0 && msg.getMessageType()==Type.CONNECTION_BUFFER_SERVER){
						if(!unknownChannels.contains(e.getChannel()))
							throw new Exception("Server received an unknown channel identification message.");
						boolean removed = unknownChannels.remove(e.getChannel());
						System.out.println("Server: " + conf.getBufferServerSocketAddress()+ " Client of Connection: " + msg.getClientSocketAddress()
								+ "Removed From UnknowList: " + removed);	
					}else
						if(unknownChannels.size()>0 && msg.getMessageType()==Type.CONNECTION_DB_CLIENT){
							if(!unknownChannels.contains(e.getChannel()))
								throw new Exception("Server received an unknown channel identification message.");
							boolean removed = unknownChannels.remove(e.getChannel());
							System.out.println("Server: " + conf.getBufferServerSocketAddress()+ " Client connection: " + msg.getClientSocketAddress()
									+ "Removed From UnknowList: " +	removed );
							//find the ensemble for the client: we have to add new field to proto : ensemble name
							ensemble.addHeadDBClient(msg.getEntryId().getClientId(), msg.getClientSocketAddress(), e.getChannel());
						}else
							if(msg.getMessageType()==Type.TAIL_NOTIFICATION){
								Channel tailChannel = client.connectServerToClient(NetworkUtil.parseInetSocketAddress(msg.getClientSocketAddress()));
								ensemble.addTailDBClient(msg.getEntryId().getClientId(), tailChannel);
								System.out.println("TAIL MSG: " + msg.getEntryId() + " Tail add:" + conf.getBufferServerSocketAddress());
							}

			}else
			{
				ensemble.addToBuffer(msg);
				System.out.println("1Server added to buffer: " + msg.getEntryId() + "Server: " + conf.getBufferServerSocketAddress());
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
		closeOnFlush(e.getChannel());
	}

	static void closeOnFlush(Channel ch) {
		if (ch.isConnected()) {
			ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
		}
	}

	public void close(){
		server.stop();
		client.stop();
	}

}
