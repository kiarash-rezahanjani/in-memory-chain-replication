package client;

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
	IDGenerator idGenerator = new IDGenerator();
	LatencyEvaluator latencyEvaluator;
	Object sendLock;
	
	public LoadGenerator(Channel headServer, Configuration conf, LatencyEvaluator latencyEvaluator, int timeInterval, int size, Object sendLock){
		this.sendLock = sendLock;
		this.timeInterval = timeInterval; 
		this.size = size;
		this.conf = conf;
		this.latencyEvaluator = latencyEvaluator;
		this.headServer = headServer;
		for(int i=0 ; i<this.size; i++){
			payload += "o";
		}
	}
	
	public LoadGenerator(Channel headServer, Configuration conf, LatencyEvaluator latencyEvaluator, Object sendLock){
		this( headServer, conf , latencyEvaluator, default_timeInterval, default_size, sendLock);
	}
	
	public void startLoad(){
		load = true;
	}
	
	public void stopLoad(){
		load = false;
	}
	
	public void stopRunning(){
		running = false;
	}
	
	public void run(){
		
		while(running){
			while(!load && running){
				try {
					sleep(1000);
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
			latencyEvaluator.sent(id);//start elapsed time
	//		System.out.println("Channel " + headServer + " entry " + entry);
			headServer.write(entry).awaitUninterruptibly();
		//	System.out.println("Sent " + entry.getEntryId().getMessageId() );
			synchronized(sendLock){try {
				sendLock.wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}}
	/*		try {
				Thread.sleep(timeInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
*/		}
	}
}
