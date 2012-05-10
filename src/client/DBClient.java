package client;

import java.io.FileNotFoundException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;

import streaming.BufferClient;
import streaming.BufferServer;
import utility.Configuration;
import utility.LatencyEvaluator;
import ensemble.ClientServerCallback;
import ensemble.Ensemble;

public class DBClient implements Watcher{

	Configuration conf;
	BufferServer server;
	BufferClient client;
	InetSocketAddress headSocketAddress;
	//List<Identifier> sentMessages = new ArrayList<Identifier>();
	//List<Identifier> receivedMessages = new ArrayList<Identifier>();
	Channel headServer ;
	Channel tailServer ;
	int connRetry  = 3;//millisecond
	LoadGenerator loadGeneratorThread ;
	LatencyEvaluator latencyEvaluator ;
	final Semaphore semaphore = new Semaphore(1);
	IDGenerator idGenerator = new IDGenerator();
	//boolean running=true;
	//volatile boolean stop = false; 
	static ZooKeeper zk = null;
	HashMap<InetSocketAddress, ServerRole> ensembleServers = new HashMap<InetSocketAddress, ServerRole>();
	Hashtable<Identifier, LogEntry> bufferedMessage = new Hashtable<Identifier, LogEntry>();

	ClientServerCallback callback = new ClientServerCallback() {
		@Override
		public void serverReceivedMessage(MessageEvent e) {
			// TODO Auto-generated method stub
			LogEntry msg = (LogEntry) e.getMessage();
			if(msg.hasMessageType()){//check if this is a channel identification message
				//replace with a switch
				if(msg.getMessageType()==Type.ACK){
					bufferedMessage.remove(msg.getEntryId());
					semaphore.release();
					latencyEvaluator.received(msg.getEntryId());
					System.out.println("Ack of " + msg.getEntryId().getMessageId()  + " loadG status: "+  loadGeneratorThread.isAlive() + loadGeneratorThread.isInterrupted());
					//receivedMessages.add(msg.getEntryId());
					if(msg.getEntryId().getMessageId()%1000 ==999)
						System.out.println("Acked: " + msg.getEntryId().getMessageId());

					if(msg.getEntryId().getMessageId()==10000){
						loadGeneratorThread.stopLoad();
						latencyEvaluator.report();
						close();
					}
					return;
				}
				if(msg.getMessageType()==Type.CONNECTION_TAIL_TO_DB_CLIENT){
					tailServer = e.getChannel();
					System.out.println("Ready to stream." + msg.getClientSocketAddress() );
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
			System.out.println("Client received Exception: " + e.getChannel());
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
			}else
			{
				System.out.println("\n\nClosing the head and tail channels " +  ch + " by "  );
				headServer.close();
				tailServer.close();
			}
		}
	};

	public void close(){
		headServer.close();
		tailServer.close();
		loadGeneratorThread.stopRunning();
		server.stop();
		client.stop();
	}

	public DBClient(Configuration conf){
		this.conf = conf;
		server = new BufferServer(conf, callback);
		client = new BufferClient(conf, callback);
		latencyEvaluator = new LatencyEvaluator();
		loadGeneratorThread = new LoadGenerator( conf, 0, 200, semaphore, idGenerator, bufferedMessage);
		if(zk==null){
			try {
				zk = new ZooKeeper(conf.getZkConnectionString(), conf.getZkSessionTimeOut(), this);
				System.out.println("connectin to zk...");
				createRoot(conf.getZkNameSpace());
				createRoot(conf.getZkNameSpace()+conf.getZkClientRoot());

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
	}	


	//for testing
	public DBClient(Configuration conf, String serverHost, int serverPort){
		this(conf);
		this.headSocketAddress = new InetSocketAddress(serverHost, serverPort);
	}

	public void stop(){
		server.stop();
		client.stop();
		loadGeneratorThread.stopRunning();
	}


	public void run() {
		headSocketAddress = getWatchedEnsemble();
		while( tailServer==null || headServer==null || !headServer.isConnected() ||  !tailServer.isConnected()){
			try {
				System.out.println("Client connectiong to " + headSocketAddress);
				if(headServer==null || !headServer.isConnected())
					headServer = client.connectClientToServer(headSocketAddress);
				Thread.sleep(connRetry);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Head Channel " + headServer);
		System.out.println("Tail Channel " + tailServer);
		flushBuffer();
		loadGeneratorThread.setHeadServer(headServer);
		loadGeneratorThread.setEvaluator(latencyEvaluator);
		loadGeneratorThread.startLoad();
		loadGeneratorThread.start();
		
	}
	
	void flushBuffer(){
		System.out.println("ReSending size..." +  bufferedMessage.size() );
		if(bufferedMessage.size()==0)
			return;
		
		//sort the messages
		Iterator it = bufferedMessage.keySet().iterator();
		List<Identifier> ids = new ArrayList<Identifier>();
		while(it.hasNext()){
			Identifier id = (Identifier) it.next();
			if(ids.size()==0){ids.add(id); continue;}
			for(int i = 0; i< ids.size(); i++){
				if(ids.get(i).getMessageId()>id.getMessageId())
				{	ids.add(i, id); break;}
				ids.add(id);
			}
		}
		
		
		for(int i = 0; i< ids.size(); i++){
			System.out.println("sending....." +  ids.get(i) );
			headServer.write(bufferedMessage.get(ids.get(i))).awaitUninterruptibly();
			System.out.println("sent....." +  ids.get(i) );
		}

	}
	//==================Zookeeper and Failure===============================
	public void ensembleFailure(ServerRole serverRole){
		//pause the stream
		//close the channels 
		//send the last acked entry to the servers
		//find ensemble and set watch on the servers
		//connect to the ensemble : jus find the head and connect
		//resume the service
		System.out.println("EnsembleFailure: Stopping... " );
		//stop();


		//		loadGeneratorThread.stopRunning();

		loadGeneratorThread.stopLoad();
		headSocketAddress=null;
		headServer.close();
		tailServer.close();
		headServer=null;
		tailServer=null;
		System.out.print("Running...." );
		run();
	}

	public Stat setFailureDetector(String nodeName)
	{
		Stat stat=null;
		try {
			stat = zk.exists(conf.getZkNameSpace() + conf.getZkServersRoot() + "/" + nodeName, true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		} 
		return stat;
	}

	enum ServerRole{
		head, tail, middle
	}

	void createRoot(String path) throws KeeperException, InterruptedException{
		try {
			Stat s = zk.exists(path, false);
			if(s==null)
				zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		System.out.print("SSSSSsome one failed: " + event.getPath());
		if (event.getType()== Event.EventType.NodeDeleted){
			if(!path.contains(conf.getZkServersRoot())){
				System.out.print("strange failure messsage received at client.");
				System.exit(-1);
			}
			System.out.print("Its a server. " );
			//String nodeName =  path.substring( path.substring(path.indexOf(conf.getZkServersRoot())+1).indexOf("/")+1 );
			Iterator it = ensembleServers.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<InetSocketAddress, ServerRole> entry = (Map.Entry<InetSocketAddress, ServerRole>) it.next();
				String serverZnodePath = conf.getZkNameSpace() + conf.getZkServersRoot() + "/" + getHostColonPort(entry.getKey().toString());
				if(serverZnodePath.equals(path)){
					ensembleFailure(entry.getValue());
				}
			}
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

	int i = 0; //just for testing
	InetSocketAddress getWatchedEnsemble(){
		i++;
		ensembleServers.clear();
		//contact zookeeper and get an ensemble
		//determine the head and tail
		HashMap<InetSocketAddress, ServerRole> ensembleServers1 = new HashMap<InetSocketAddress, ServerRole>();
		ensembleServers1.put(new InetSocketAddress( "gsbl90151", 2111), ServerRole.head);
		ensembleServers1.put(new InetSocketAddress( "gsbl90152", 2112), ServerRole.middle);
		ensembleServers1.put(new InetSocketAddress( "gsbl90153", 2113), ServerRole.tail);

		HashMap<InetSocketAddress, ServerRole> ensembleServers2 = new HashMap<InetSocketAddress, ServerRole>();
		ensembleServers2.put(new InetSocketAddress( "gsbl90154", 2111), ServerRole.head);
		ensembleServers2.put(new InetSocketAddress( "gsbl90155", 2112), ServerRole.middle);
		ensembleServers2.put(new InetSocketAddress( "gsbl90156", 2113), ServerRole.tail);

		HashMap<InetSocketAddress, ServerRole> ensembleServers3 = new HashMap<InetSocketAddress, ServerRole>();
		ensembleServers3.put(new InetSocketAddress( "gsbl90157", 2111), ServerRole.head);
		ensembleServers3.put(new InetSocketAddress( "gsbl90158", 2112), ServerRole.middle);
		ensembleServers3.put(new InetSocketAddress( "gsbl90159", 2113), ServerRole.tail);
		if(i%2==0) 
			ensembleServers = ensembleServers1 ;
		if(i%2==1) 
			ensembleServers = ensembleServers2 ;
		//if(i%3==2) 
		//	ensembleServers = ensembleServers3 ;

		//set watched on the servers and set the head server
		Iterator it = ensembleServers.entrySet().iterator();
		InetSocketAddress head = null;
		Stat stat = null;
		while(it.hasNext()){
			Map.Entry<InetSocketAddress, ServerRole> entry =  (Map.Entry<InetSocketAddress, ServerRole>) it.next();
			stat = setFailureDetector(getHostColonPort(entry.getKey().toString()));
			if(stat==null)
				try {
					throw new Exception("Hey one of the servers in down buddy.");
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(-1);
				}
			if(entry.getValue()==ServerRole.head)
				head = entry.getKey();
		}

		return head;
	}


	//copied from zookeeperclient class
	public String getHostColonPort(String socketAddress)
	{
		String host = "";
		int port=0;

		Pattern p = Pattern.compile("^\\s*(.*?):(\\d+)\\s*$");
		Matcher m = p.matcher(socketAddress);

		if (m.matches()) 
		{
			host = m.group(1);

			if(host.contains("/"))
				host = host.substring( host.indexOf("/") + 1 );

			port = Integer.parseInt(m.group(2));

			return host + ":" + port;
		}else
			return null;

	}


}

