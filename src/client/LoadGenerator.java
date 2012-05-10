package client;

import java.util.Hashtable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.channel.Channel;

import utility.Configuration;
import utility.LatencyEvaluator;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;

public class LoadGenerator extends Thread{

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
	
	public LoadGenerator( Configuration conf, int timeInterval, int size, final Semaphore semaphore, IDGenerator idGenerator, Hashtable<Identifier, LogEntry> sentMessage ){
		this.idGenerator = idGenerator;
		this.semaphore = semaphore;
		this.timeInterval = timeInterval; 
		this.size = size;
		this.conf = conf;
		this.sentMessage = sentMessage;
	
		for(int i=0 ; i<this.size; i++){
			payload += "o";
		}
	}
	public void setHeadServer(Channel headServer){ this.headServer = headServer;}
	public void setEvaluator(LatencyEvaluator latencyEvaluator){ this.latencyEvaluator = latencyEvaluator;}

	public LoadGenerator(Channel headServer, Configuration conf, LatencyEvaluator latencyEvaluator, int timeInterval, int size, final Semaphore semaphore, IDGenerator idGenerator,Hashtable<Identifier, LogEntry> sentMessage  ){
		this.idGenerator = idGenerator;
		this.semaphore = semaphore;
		this.timeInterval = timeInterval; 
		this.size = size;
		this.conf = conf;
		this.latencyEvaluator = latencyEvaluator;
		this.headServer = headServer;
		this.sentMessage = sentMessage ;
		for(int i=0 ; i<this.size; i++){
			payload += "o";
		}
	}
	
	public LoadGenerator(Channel headServer, Configuration conf, LatencyEvaluator latencyEvaluator, final Semaphore semaphore, IDGenerator idGenerator, Hashtable<Identifier, LogEntry> sentMessage ){
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
	
	public void run(){		
		
		while(running){
			while(!load && running){
				try {
					System.out.println("LoadGenerator innerloop: run and load "+ (running) + " " + (load));
					sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			long msgId = idGenerator.getNextId();
			Identifier id = Identifier.newBuilder()
					.setClientId(conf.getDbClientId())
					.setMessageId(msgId).build();
			LogEntry entry = LogEntry.newBuilder()
					.setEntryId(id)
					.setKey("Key:"+msgId)
					.setClientSocketAddress(conf.getBufferServerSocketAddress().toString())
					.setOperation(payload).build();
			
			
			System.out.println("sending "+ entry.getEntryId().getMessageId());
			
			try {
				semaphore.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Semaphore Acquired");
			sentMessage.put(entry.getEntryId(), entry);//put in the buffer
			latencyEvaluator.sent(id);//start elapsed time
			headServer.write(entry).awaitUninterruptibly();

			System.out.println("sent "+ entry.getEntryId().getMessageId());
			
		}
		System.out.println("LoadGenerator ended: run and load "+ (running) + " " + (load));
	}
	

}
