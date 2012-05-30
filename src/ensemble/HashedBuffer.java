package ensemble;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;


public class HashedBuffer implements  Buffer{
	Hashtable<Identifier, LogEntry> data ;
	BlockingQueue<Identifier> readQueue ;
	BlockingQueue<Identifier> persistQueue ;
	Set<String> persistClient;
	Hashtable<String, HashSet<Identifier>> clientMessages ; 
	ExecutorService executor = Executors.newSingleThreadExecutor();
	Hashtable<String, Boolean> failedClientGarbageCollectionStatus = new Hashtable<String, Boolean>();
	Object full = new Object();
	Object empty = new Object();

	private int capacity;

	public HashedBuffer(int capacity, Set<String> persistClient){
		this.capacity = capacity;
		this.persistClient = persistClient;
		data = new Hashtable<Identifier, LogEntry>(capacity);
		readQueue = new LinkedBlockingQueue<Identifier>(capacity);
		persistQueue = new LinkedBlockingQueue<Identifier>(capacity);
		clientMessages = new Hashtable<String, HashSet<Identifier>>();
	}
	
	public int size(){
		return data.size();
	}

	public void add(Object object) {
		// TODO Auto-generated method stub
		add((LogEntry) object);
	}

	public void add(LogEntry entry) {
		// TODO Auto-generated method stub
		while(data.size()>=capacity ){
			synchronized(full){
				try {
						full.wait();	
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		if(!clientMessages.keySet().contains(entry.getEntryId()))
			clientMessages.put(entry.getEntryId().getClientId(), new HashSet<Identifier>(capacity));

		clientMessages.get(entry.getEntryId().getClientId()).add(entry.getEntryId());
		data.put(entry.getEntryId(), entry);
		readQueue.offer(entry.getEntryId());
		report("added to buffer " + entry.getEntryId().getMessageId());
	}

	public LogEntry nextToRead() {
		// TODO Auto-generated method stub
		LogEntry entry = null;
		do{
			try {
				entry = data.get(readQueue.take());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}while(entry==null);
		report("Reading " + entry.getEntryId().getMessageId());
		return entry;
	}

	public void readComplete(Identifier id) {
		// TODO Auto-generated method stub
/*		if(persistClient.contains(id.getClientId()))*/{
			persistQueue.offer(id);
			report("added to persistQ " + id.getMessageId());
		}
	}

	public LogEntry nextToPersist() {
		// TODO Auto-generated method stub
		LogEntry entry = null;
		do{
			try {
				entry = data.get(persistQueue.take());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}while(entry==null);
		return entry;
	}

	public void remove(Identifier id) {
		// TODO Auto-generated method stub

		clientMessages.get(id.getClientId()).remove(id);
		data.remove(id);
		synchronized(full){
			full.notifyAll();
		}
		report("removed " + id.getMessageId());
	}
	
	/**
	 * this method remove the elements from the read queue and add to persist queue
	 * this method should be called only when the ensemble is broken so that all the buffered logs will be persisted.
	 * 
	 */
	public void fillPersistQueue() {
		// TODO Auto-generated method stub
		Thread persistAll = new FillPersistQueue();
		persistAll.start();
	}
	class FillPersistQueue extends Thread{
		public void run(){
			while(readQueue.size()>0){
				persistQueue.offer( readQueue.poll() );
			}
		}
	}

	public void garbageCollect(Identifier lastAckedId){
		failedClientGarbageCollectionStatus.put(lastAckedId.getClientId(), Boolean.valueOf(false));
		executor.submit(new BufferGarbageCollector(lastAckedId));
		report("gc starts!");
	}

	/**
	 * Remove all the entries with messageId greater than the given messageID belong to the given client.
	 * Once this runnable is done it sets the status of the given client to true.
	 * @author root
	 *
	 */
	class BufferGarbageCollector implements Runnable{
		Identifier lastAckedId;
		public BufferGarbageCollector(Identifier lastAckedId){
			this.lastAckedId = lastAckedId;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			HashSet<Identifier> idSet = clientMessages.get(lastAckedId.getClientId());
			Iterator<Identifier> it = idSet.iterator();
			while(it.hasNext()){
				Identifier id = it.next();
				if(id.getMessageId() > lastAckedId.getMessageId())
					remove(id);
			}
			//clientMessages.get(lastAckedId.getClientId()).clear();// remove all the ids belong to t he client but
			failedClientGarbageCollectionStatus.put(lastAckedId.getClientId(), Boolean.valueOf(true));
			report("gc done!");

		}
	}

	public void report(String msg){
	//	System.out.println(msg + "\n RQ " + readQueue.size() + " PQ " +  persistQueue.size() + " Data " +  data.size() );
	}
/*
	public static void main(String[] args) {
		Set<String> tail = new HashSet<String>();
		tail.add("cli");
		HashedBuffer buffer = new HashedBuffer(200,tail);

		Read read = new Read(buffer,100);
		Persist persist = new Persist(buffer,200);
		Add add = new Add(buffer, 20);
		read.start();
		persist.start();
		add.start();

		//	int removeIndx s0;
	}
*/
}
