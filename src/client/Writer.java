package client;

import java.io.IOException;
import java.util.Hashtable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;

import utility.Configuration;
import utility.LatencyEvaluator;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;

public class Writer extends Thread implements Logger{

	public static final int default_timeInterval = 10; 
	public static final int default_size = 200;
	int timeInterval ; 
	int size ;
	Channel headServer; 
	boolean running = true;
	boolean load = false;
	DBClient dbClient;
	String payload = "";
	Configuration conf;
	IDGenerator idGenerator;
	LatencyEvaluator latencyEvaluator;
	final Semaphore semaphore;
	Hashtable<Identifier, LogEntry> sentMessage;
	BlockingQueue<KVPair> entryQueue = new LinkedBlockingQueue<KVPair>();
	
	public Writer( Configuration conf, int timeInterval, int size, final Semaphore semaphore, IDGenerator idGenerator, Hashtable<Identifier, LogEntry> sentMessage ){
		this.idGenerator = idGenerator;
		this.semaphore = semaphore;
		this.timeInterval = timeInterval; 
		this.size = size;
		this.conf = conf;
		this.sentMessage = sentMessage;
	
		for(int i=0 ; i<conf.getValueSize(); i++){
			payload += "o";
		}
	}
	
	public void setHeadServer(Channel headServer){ this.headServer = headServer;}
	public void setEvaluator(LatencyEvaluator latencyEvaluator){ this.latencyEvaluator = latencyEvaluator;}

	public Writer(Channel headServer, Configuration conf, LatencyEvaluator latencyEvaluator, int timeInterval, int size, final Semaphore semaphore, IDGenerator idGenerator,Hashtable<Identifier, LogEntry> sentMessage  ){
		this.idGenerator = idGenerator;
		this.semaphore = semaphore;
		this.timeInterval = timeInterval; 
		this.size = size;
		this.conf = conf;
		this.latencyEvaluator = latencyEvaluator;
		this.headServer = headServer;
		this.sentMessage = sentMessage ;
		String _100B = "";
		for(int i=0 ; i<100; i++){
			_100B += "o";
		}
		for(int i=0 ; i<this.size; i++){
			payload += _100B;
		}
	}
	
	public Writer(Channel headServer, Configuration conf, LatencyEvaluator latencyEvaluator, final Semaphore semaphore, IDGenerator idGenerator, Hashtable<Identifier, LogEntry> sentMessage ){
		this( headServer, conf , latencyEvaluator, default_timeInterval, default_size, semaphore, idGenerator, sentMessage);
	}
	
	public void startLoad(){
		load = true;
	}
	
	public void stopLoad(){
		load = false;
	}
	
	public void stopRunning(){
		running = false;
		interrupt();
	}
	
	
	private LogEntry nextEntry(){
		KVPair kv = null;
		try {
			kv = entryQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		long msgId = idGenerator.getNextId();
		Identifier id = Identifier.newBuilder()
				.setClientId(conf.getDbClientId())
				.setMessageId(msgId).build();
		LogEntry entry = LogEntry.newBuilder()
				.setEntryId(id)
				.setKey(kv.getKey())
				.setClientSocketAddress(conf.getBufferServerSocketAddress().toString())
				.setOperation(kv.getValue()).build();
		return entry;
	}
	
	Object sync = new Object();
	public void addEntry(String key, String value){
		entryQueue.add(new KVPair(key, value));
		synchronized(sync){try {
			sync.wait();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
		}}
	}
	
	public void run(){		
		while(running){
			while(!load && running){
				try {
					System.out.println("LOAD STOPPED. Writer innerloop: run and load "+ (running) + " " + (load));
					sleep(2);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			try {
				semaphore.acquire();
				synchronized(sync){sync.notifyAll();}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
			LogEntry entry = nextEntry();
			sentMessage.put(entry.getEntryId(), entry);//put in the buffer
			latencyEvaluator.sent(entry.getEntryId());//start elapsed time
		//	headServer.write(entry).awaitUninterruptibly();
			headServer.write(entry);
	
		}
		System.out.println("Writer ended: run and load flag "+ (running) + " " + (load));
	}

}
