package client;

import java.net.InetSocketAddress;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;

import streaming.BufferClient;
import streaming.BufferServer;
import utility.Configuration;
import ensemble.ClientServerCallback;

public class DBClient implements Runnable{

	Configuration conf;
	BufferServer server;
	BufferClient client;
	InetSocketAddress serverSocketAddress;
	volatile boolean stop = false; 
	ClientServerCallback callback = new ClientServerCallback() {

		@Override
		public void serverReceivedMessage(MessageEvent e) {
			// TODO Auto-generated method stub
			LogEntry msg = (LogEntry) e.getMessage();
			if(msg.hasMessageType()){//check if this is a channel identification message
				//replace with a switch
				if(msg.getMessageType()==Type.ACK){
					System.out.println("Client Rec Ack: " + msg.getEntryId() + " Cli: " + conf.getBufferServerSocketAddress() 
							+ " From: " + msg.getClientSocketAddress());
					return;
				}
				if(msg.getMessageType()==Type.CONNECTION_TAIL_TO_DB_CLIENT){
					System.out.println("Tail to Db Client: from" + msg.getClientSocketAddress() );

					return;
				}
			}
			System.out.println("Client got a wiered message");
		}

		@Override
		public void serverAcceptedConnection(ChannelStateEvent e) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void exceptionCaught(ExceptionEvent e) {
			// TODO Auto-generated method stub
			System.out.println("Client accepted connection: " + e.getChannel());
		}

		@Override
		public void clientReceivedMessage(MessageEvent e) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void channelClosed(ChannelStateEvent e) {
			// TODO Auto-generated method stub
			System.out.println("Client receievd closed.");
		}
	};

	public DBClient(Configuration conf){
		this.conf = conf;
		server = new BufferServer(conf, callback);
		client = new BufferClient(conf, callback);
	}

	//for testing
	public DBClient(Configuration conf, String serverHost, int serverPort){
		this(conf);
		this.serverSocketAddress = new InetSocketAddress(serverHost, serverPort);
	}
	
	public void stop(){
		stop = true;
	}

	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		Channel channel = client.connectClientToServer(serverSocketAddress);
		//while(!stop){
			for(int i = 0; i<20 ; i++){
				Identifier id = Identifier.newBuilder()
						.setClientId(conf.getDbClientId())
						.setMessageId(IDGenerator.getNextId()).build();
						
				LogEntry entry = LogEntry.newBuilder()
						.setEntryId(id)
						.setKey("Key"+i)
						.setClientSocketAddress(conf.getBufferServerSocketAddress().toString())
						.setOperation("Opt.add(pfffff)").build();
				
				ChannelFuture f = channel.write(entry);

				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if(f.awaitUninterruptibly().isSuccess())
					;
				else
					;
			}
		//}
	
	}



}
