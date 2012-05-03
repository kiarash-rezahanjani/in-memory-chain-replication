package JustTesting;


import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import ensemble.CircularBuffer;
import client.Log;




/**
 * Only one thread uses add and one thread uses remove method and one thread uses read.
 * One assumption is that before persist operation the entry has certainly been read 
 * meaning that its been replicated and its Ack has been send. 
 * @author root
 *
 */

public class ConcurrentCircularBuffer {

	final private LogEntry[] buffer;
	private int readPosition = 0;
	private int writePosition = 0;
	private int persistPosition = 0;
	final private int capacity ;
	private AtomicInteger size = new AtomicInteger(); 
	private HashMap<String, HashMap<Long, Integer>> logIndex = new HashMap<String, HashMap<Long, Integer>>();//<clientId,<msgId, poistion in the buffer>>
	Queue<Integer> persistIndexQueue = new LinkedList<Integer>();
	private Set<String> persistClient;//should be synchronized
	private BitSet bufferElementRead; //jus to check: each bit at position x indicates whether the element at this position is persisted
	private BitSet bufferElementPersist;
	Object full = new Object();
	Object empty = new Object();
	Object readLock = new Object();
	Object persistLock = new Object();
	boolean persisterWaitForReader = false;

	//	private Semaphore addSem ;
	//	private Semaphore removeSem ;

	public ConcurrentCircularBuffer(int capacity, Set<String> persistClientIdSet) {
		this.capacity = capacity;
		this.persistClient = persistClientIdSet;
		//		addSem = new Semaphore(capacity);
		//		removeSem = new Semaphore(capacity);
		bufferElementRead = new BitSet(this.capacity);
		bufferElementPersist = new BitSet(this.capacity);
		buffer = new LogEntry[capacity];
		readPosition = 0;
		writePosition = 0;
		size.set(0);
	}

	public void add(Object obj) {
		add((LogEntry)obj);
	}
	// add and remove client and create neccessary data struct
	public void add(LogEntry entry) {

		while(size.get()>=capacity ||  bufferElementPersist.get(writePosition)){
			synchronized(full){
				try {
					if(size.get()>=capacity){
						print1("full: waiting thread:" + "size.get()>=capacity " + (size.get()>=capacity) + " bufferElementPersist.get(writePosition) "+(bufferElementPersist.get(writePosition)) );
						full.wait();
						print1("full: release:" + "size.get()>=capacity " + (size.get()>=capacity) + " bufferElementPersist.get(writePosition) "+(bufferElementPersist.get(writePosition)) );
					}

				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		String clientId = entry.getEntryId().getClientId();
		Long msgId = Long.valueOf(entry.getEntryId().getMessageId());
	//	Integer writePosition = Integer.valueOf(writePosition);


		if (size.get()<capacity) {//it should never happens remove the condition later
			if(!logIndex.containsKey(clientId)){//move to add a client method later
				HashMap<Long,Integer> messages = new HashMap<Long, Integer>();
				messages.put(msgId, Integer.valueOf(writePosition));
				logIndex.put(clientId, messages);
			}else
				logIndex.get(clientId).put(msgId, Integer.valueOf(writePosition));

			bufferElementRead.set(writePosition, true);
			bufferElementPersist.set(writePosition, true);
			print(String.valueOf(persistClient.contains(clientId)) + " this.list "+persistClient.contains(clientId) + " arg: " + clientId);
			if(persistClient.contains(clientId)){
				persistIndexQueue.offer(Integer.valueOf(writePosition));
				//if(persistIndexQueue.size()==1)
				print("NOTIFY PERSIST");
					synchronized(persistLock){
						persistLock.notifyAll();
					}
				
			}
			buffer[writePosition++] = entry;
			writePosition = writePosition % capacity;
			size.incrementAndGet();
		} else {

			//we can say wait till its available
			throw new BufferOverflowException();
		}

		///|| readPosition+2 == writePosition || persistIndexQueue.size()==1
		//if(size.get()==1)
		{
			synchronized(empty){
				empty.notifyAll();
			}
		}

		//if(writePosition-readPosition==1 )
		{
			synchronized(readLock){
				readLock.notifyAll();
			}
		}
		print("Added " +  Thread.currentThread());
	}



	public void readComplete(Identifier id){

		Integer position = logIndex.get(id.getClientId()).get(Long.valueOf(id.getMessageId()));
		if(position==null)
			try {
				throw new Exception("Position null.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		bufferElementRead.set(position.intValue(), false);
		if(persisterWaitForReader == true)
			synchronized (persistLock) {
				print("Notify Persister by thread:" +  Thread.currentThread());
				persistLock.notifyAll();
			}
	}

	public LogEntry nextToRead() {
		while(bufferElementRead.get(readPosition)==false){
			try {
				synchronized (readLock) {
					print("ReadLock: waiting thread:" +  Thread.currentThread());
					readLock.wait();
					print("Not ReadLock: waiting thread:" +  Thread.currentThread());
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		LogEntry t = null;
		if (size.get()>0) {//should not happen we can remove later
			if(bufferElementRead.get(readPosition)==false)//should not happen
				try {
					throw new Exception("Read element which doesnt exist. bufferElementRead.get(position)==false.");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			t = (LogEntry) buffer[readPosition];
			//	bufferElementRead.set(readPosition, false);
			readPosition = ++readPosition % capacity;//potential bug

		} else {
			try {
				throw new Exception(String.valueOf(size.get()> 0) + String.valueOf(readPosition!=writePosition));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//bufferElementRead.set(readPosition, false);
		print("Read " +  Thread.currentThread());
		return t;
	}

	public void remove(Identifier id){	
		String clientId = id.getClientId(); 
		long msgId = id.getMessageId();
		Integer position = logIndex.get(clientId).get(Long.valueOf(msgId));
		if(position==null)
			try {
				throw new Exception("Position null. Wrong element to remove.");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.exit(-1);
			}
		//size.get()<=0  condition should never happens as the id correspondent entry must be in the buffer
		while(bufferElementRead.get(position.intValue())){
			synchronized(persistLock){
				try {
					print1("persist: waiting:" +  " bufferElementRead.get(position.intValue() "+bufferElementRead.get(position.intValue()));
					persistLock.wait();
					print1("persist: release:" +  " bufferElementRead.get(position.intValue() "+bufferElementRead.get(position.intValue()));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		Object obj = logIndex.get(clientId).remove(Long.valueOf(msgId));
		if(obj!=null){
			size.decrementAndGet();
			bufferElementPersist.set(position.intValue(), false);
			persistPosition = position.intValue();
			synchronized (full) {
				full.notifyAll();}
		}
		print1("Persisted & Removed " + id.getMessageId()+ " " +  Thread.currentThread());
	}
/**
 * 
 * @return
 */
	public LogEntry nextToPersist() {
		// TODO Auto-generated method stub
		//if element is not read can not be persisted
		while(persistIndexQueue.size()<=0 || bufferElementRead.get(persistIndexQueue.peek()) || !bufferElementPersist.get(persistIndexQueue.peek()) ){
			synchronized(persistLock){
				try {
					if(persistIndexQueue.size()>0)
					print1("persistlock: waiting thread:" +  "persistIndexQueue.size()<=0 " + String.valueOf(persistIndexQueue.size()<=0) + " bufferElementRead.get(persistIndexQueue.peek()) " + String.valueOf(bufferElementRead.get(persistIndexQueue.peek())) 
						+ " !bufferElementPersist.get(persistIndexQueue.peek()) " + String.valueOf(!bufferElementPersist.get(persistIndexQueue.peek()))  );
					
					persisterWaitForReader = true;
					persistLock.wait();
					if(persistIndexQueue.size()>0)
					print1("persistlock: release thread:" +  "persistIndexQueue.size()<=0 " + String.valueOf(persistIndexQueue.size()<=0) + " bufferElementRead.get(persistIndexQueue.peek()) " + String.valueOf(bufferElementRead.get(persistIndexQueue.peek())) 
							+ " !bufferElementPersist.get(persistIndexQueue.peek()) " + String.valueOf(!bufferElementPersist.get(persistIndexQueue.peek()))  );
							
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}	
		persisterWaitForReader = false;
		print("Persist Only Read " +  Thread.currentThread());
		return buffer[persistIndexQueue.poll().intValue()];
	}

	public String toString() {
		return "CircularBuffer(capacity=" + buffer.length + 
				", write=" + writePosition + ", read=" + readPosition + 
				" Size=" + size.get() + 
				" HASH: " + logIndex.toString();
	}

	void print(String str){
		String meta = "\n[RP: "+readPosition + " WP: " + writePosition + " PS: " + 
				persistIndexQueue.size() + " c/s: " + size.get() + "/"+ capacity + " \n"; 
	//	System.out.println("BUFFER "+str + "  " + Thread.currentThread().getName() + meta);
	}
	void print1(String str){
		String meta = "\n[RP: "+readPosition + " WP: " + writePosition + " PQSz: " + 
				persistIndexQueue.size() + " Persist Pos: " + persistPosition + " c/s: " + size.get() + "/"+ capacity + " \n" ; 
		System.out.println("BUFFER "+str + "  " + Thread.currentThread().getName() + meta);
	}
	public static void main(String[] args) {
		Set<String> tail = new HashSet<String>();
		tail.add("cli");
		ConcurrentCircularBuffer  buffer = new ConcurrentCircularBuffer(200,tail);

		Read read = new Read(buffer,200);
		Persist persist = new Persist(buffer,100);
		Add add = new Add(buffer, 50);
		read.start();
		persist.start();
		add.start();
		
		
		
		//	int removeIndx s0;

	}


}

