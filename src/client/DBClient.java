package client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
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

public class DBClient extends Thread{

	Configuration conf;
	BufferServer server;
	BufferClient client;
	InetSocketAddress serverSocketAddress;
	List<Identifier> sentMessages = new ArrayList<Identifier>();
	List<Identifier> receivedMessages = new ArrayList<Identifier>();
	Channel headServer;
	Channel tailServer;
	int connRetry  = 5;//milisecond
	boolean running=true;
	volatile boolean stop = false; 

	ClientServerCallback callback = new ClientServerCallback() {
		@Override
		public void serverReceivedMessage(MessageEvent e) {
			// TODO Auto-generated method stub
			LogEntry msg = (LogEntry) e.getMessage();
			if(msg.hasMessageType()){//check if this is a channel identification message
				//replace with a switch
				if(msg.getMessageType()==Type.ACK){
					System.out.println("Rec Ack of Message " + msg.getEntryId().getMessageId() 	+ " From: " + msg.getClientSocketAddress());
					receivedMessages.add(msg.getEntryId());
					if(msg.getEntryId().getMessageId()==55){
					e.getChannel().disconnect();
						System.exit(-1);
					}
					return;
				}
				if(msg.getMessageType()==Type.CONNECTION_TAIL_TO_DB_CLIENT){
					tailServer = e.getChannel();
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
			closeOnFlush(e.getChannel(), e.getCause().toString());
		}

		@Override
		public void clientReceivedMessage(MessageEvent e) {
			// TODO Auto-generated method stub

		}

		@Override
		public void channelClosed(ChannelStateEvent e) {
			// TODO Auto-generated method stub
			System.out.println("Client receievd closed.");
			closeOnFlush(e.getChannel(), e.getValue().toString());
		}

		void closeOnFlush(Channel ch, String cause) {
			if (ch.isConnected()) {
				System.out.println("\n\nClosing the channel " +  ch + " by "  );
				ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
			}
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

	public void stopRunning(){
		running = false;
		//	interrupt();
	}


	@Override
	public void run() {
		while(running){
			//Get the head and tail address and connect to head
			IDGenerator idGenerator = new IDGenerator();
			headServer = client.connectClientToServer(serverSocketAddress);
			
			while( tailServer==null || headServer==null || !headServer.isConnected() ||  !tailServer.isConnected()){
				try {
					Thread.sleep(connRetry);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			for(long i = 0;  running; i++){
				Identifier id = Identifier.newBuilder()
						.setClientId(conf.getDbClientId())
						.setMessageId(idGenerator.getNextId()).build();

				LogEntry entry = LogEntry.newBuilder()
						.setEntryId(id)
						.setKey("Key"+i)
						.setClientSocketAddress(conf.getBufferServerSocketAddress().toString())
						.setOperation("Opt.add(pfffff)").build();

				System.out.println("Sending " + entry.getEntryId().getMessageId() );

				headServer.write(entry).awaitUninterruptibly();

				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		}
	}
}
