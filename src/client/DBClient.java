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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
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

import com.google.protobuf.InvalidProtocolBufferException;

import coordination.Znode.EnsembleData;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;

import streaming.BufferClient;
import streaming.BufferServer;
import utility.Configuration;
import utility.LatencyEvaluator;
import utility.NetworkUtil;
import ensemble.ClientServerCallback;
import ensemble.Ensemble;

public class DBClient implements Watcher{

	Configuration conf;
	BufferServer server;
	BufferClient client;
	InetSocketAddress headSocketAddress;
	Channel headServer ;
	Channel tailServer ;
	int connRetry  = 3;//millisecond
	Writer WriterThread ;
	LatencyEvaluator latencyEvaluator ;
	final Semaphore semaphore = new Semaphore(1);
	IDGenerator idGenerator = new IDGenerator();
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
					latencyEvaluator.received(msg.getEntryId());
					if(msg.getEntryId().getMessageId()%2000 ==1){
						latencyEvaluator.report();
						System.out.println("Acked: " + msg.getEntryId().getMessageId());
					}
					semaphore.release();

					if(msg.getEntryId().getMessageId()==2000000){
						WriterThread.stopLoad();
						latencyEvaluator.report();
						close();
					}
					return;
				}
				if(msg.getMessageType()==Type.CONNECTION_TAIL_TO_DB_CLIENT){
					tailServer = e.getChannel();
					System.out.println("Ready to stream. Tail:" + msg.getClientSocketAddress() );
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
		WriterThread.stopRunning();
		server.stop();
		client.stop();
	}

	public DBClient(Configuration conf){
		this.conf = conf;
		server = new BufferServer(conf, callback);
		client = new BufferClient(conf, callback);
		latencyEvaluator = new LatencyEvaluator("client_report/"+conf.getDbClientId());
		WriterThread = new Writer( conf, 0, 200, semaphore, idGenerator, bufferedMessage);
		if(zk==null){
			try {
				zk = new ZooKeeper(conf.getZkConnectionString(), conf.getZkSessionTimeOut(), this);
				System.out.println("connecting to zk...");
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

	public Logger logger(){
		return WriterThread;
	}
	
	//for testing
	public DBClient(Configuration conf, String serverHost, int serverPort){
		this(conf);
		this.headSocketAddress = new InetSocketAddress(serverHost, serverPort);
	}

	public void stop(){
		server.stop();
		client.stop();
		WriterThread.stopRunning();
	}

	public void run() {
		headSocketAddress = getWatchedEnsemble();
		while( tailServer==null || headServer==null || !headServer.isConnected() ||  !tailServer.isConnected()){
			try {
				System.out.println("Client connectiong to " + headSocketAddress);
				if(headServer==null || !headServer.isConnected())
					headServer = client.connectClientToServer(headSocketAddress);
				Thread.sleep(connRetry);
				Thread.sleep(4000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Head Channel " + headServer);
		System.out.println("Tail Channel " + tailServer);
		flushBuffer();
		WriterThread.setHeadServer(headServer);
		WriterThread.setEvaluator(latencyEvaluator);
		WriterThread.startLoad();
		WriterThread.start();
	}

	/*
	 * Send the the logs that have not been acknowledged to the currently connected server 
	 */
	void flushBuffer(){
		System.out.println("ReSending size..." +  bufferedMessage.size() );
		if(bufferedMessage.size()==0)
			return;

		//sort the logs based on the id
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

		//send the messages in order
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
		System.out.println("EnsembleFailure: Stopping logging... " );
		//stop();

		//		WriterThread.stopRunning();

		WriterThread.stopLoad();
		headSocketAddress=null;
		headServer.close();
		tailServer.close();
		headServer=null;
		tailServer=null;
		System.out.print("Running logging...." );
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
		if (event.getType()== Event.EventType.NodeDeleted){
			System.out.print("some one failed: " + event.getPath());
			if(!path.contains(conf.getZkServersRoot())){
				System.out.print("strange failure messsage received at client.");
				System.exit(-1);
			}
			System.out.print("Client: An EnsembleFailed.... " );
			//String nodeName =  path.substring( path.substring(path.indexOf(conf.getZkServersRoot())+1).indexOf("/")+1 );
			Iterator it = ensembleServers.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<InetSocketAddress, ServerRole> entry = (Map.Entry<InetSocketAddress, ServerRole>) it.next();
				String serverZnodePath = conf.getZkNameSpace() + conf.getZkServersRoot() + "/" + getHostColonPort(entry.getKey().toString());
				System.out.print("Compare.." +  serverZnodePath);
				if(serverZnodePath.equals(path)){
					System.out.print("Client: Opps its my ensemble.... " );
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

	InetSocketAddress getWatchedEnsemble(){
		ensembleServers.clear();
		//contact zookeeper and get an ensemble
		//determine the head and tail
		//HashMap<InetSocketAddress, ServerRole> ensembleServers = new HashMap<InetSocketAddress, ServerRole>();
		InetSocketAddress head = null;
		List<String> ensembles;
		try {
			ensembles = zk.getChildren(conf.getZkNameSpace()+conf.getZkEnsemblesRoot(), false);
		} catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		}
		EnsembleData ensemble = null;
		int capacityLeft = 0;
		int checkedEnsembles = 0;
		for(String path : ensembles){
			EnsembleData temp;
			try{
				temp = EnsembleData.parseFrom( zk.getData(conf.getZkNameSpace()+conf.getZkEnsemblesRoot()+"/"+path, false, null) );
			}catch(KeeperException e){continue;} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		//	if(temp.getStat()==EnsembleData.Status.ACCPT_CONNECTION && temp.getCapacityLeft() > capacityLeft){
				ensemble = temp;
				capacityLeft = ensemble.getCapacityLeft();
				checkedEnsembles++;
	//		}
			if(checkedEnsembles>=10){
				break;
			}
		}
		
		if(checkedEnsembles <=0 )
			return null;
		else{
			int headIndex = new Random().nextInt(ensemble.getMembersCount()) ;
			ServerRole role;
			for(int i = headIndex; i < headIndex + ensemble.getMembersCount(); i++){
				if(i==headIndex){
					role= ServerRole.head;
					head = NetworkUtil.parseInetSocketAddress( ensemble.getMembers(i%ensemble.getMembersCount()).getBufferServerSocketAddress() ) ;
				}
				else
					if(i==(headIndex+ensemble.getMembersCount()-1)%ensemble.getMembersCount())
						role = ServerRole.tail;
					else
						role = ServerRole.middle;
				ensembleServers.put( NetworkUtil.parseInetSocketAddress( ensemble.getMembers(i%ensemble.getMembersCount()).getSocketAddress() ), role);
				
			}
		}

		//set watched on the servers and set the head server
		Iterator it = ensembleServers.entrySet().iterator();
		
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
		//	if(entry.getValue()==ServerRole.head)
		//		head = entry.getKey();
		}

		return head;
	}
	
	//copied from zookeeperclient class
	public String getHostColonPort(String socketAddress){
		String host = "";
		int port=0;
		Pattern p = Pattern.compile("^\\s*(.*?):(\\d+)\\s*$");
		Matcher m = p.matcher(socketAddress);

		if (m.matches()) {
			host = m.group(1);
			if(host.contains("/"))
				host = host.substring( host.indexOf("/") + 1 );

			port = Integer.parseInt(m.group(2));
			return host + ":" + port;
		}else
			return null;

	}
	
	
	public static void main(String[] args) {
		System.out.println("Config file: " + args[0] +  System.getProperty("user.dir"));
		if(args.length<1){
			System.exit(-1);
		}
		
		DBClient dbcli =null;
		try {
			dbcli = new DBClient( new Configuration(args[0]));
			dbcli.run();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			dbcli.stop();
		//	System.exit(-1);
		}finally{
			
		}
		while(true){
		dbcli.logger().addEntry("key", "valle");
		}	//	System.out.println("DATABASEClient Terminated.");
		
	}
}

