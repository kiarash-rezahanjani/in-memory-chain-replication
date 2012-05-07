package ensemble;


import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import utility.TextFile;

import ensemble.Buffer;
import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;





/**
 * Only one thread uses add and one thread uses remove method and one thread uses read.
 * One assumption is that before persist operation the entry has certainly been read 
 * meaning that its been replicated and its Ack has been send. 
 * @author root
 *
 */

public class ConcurrentBuffer implements Buffer{

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
	boolean readerWaitForWritter = false;
	boolean emptyWait = true;

	//	private Semaphore addSem ;
	//	private Semaphore removeSem ;

	public ConcurrentBuffer(int capacity, Set<String> persistClientIdSet) {
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
						//				print1("full: waiting thread:" + "size.get()>=capacity " + (size.get()>=capacity) + " bufferElementPersist.get(writePosition) "+(bufferElementPersist.get(writePosition)) );
						String meta = "\n[RP: "+readPosition + " WP: " + writePosition + " PQSz: " + 
								persistIndexQueue.size() + " Persist Pos: " + persistPosition + " c/s: " + size.get() + "/"+ capacity + " \n" ; 
						System.out.println("Buffer is Full" +  buffer.length + " " + meta);
						full.wait();
						
						//				print1("full: release:" + "size.get()>=capacity " + (size.get()>=capacity) + " bufferElementPersist.get(writePosition) "+(bufferElementPersist.get(writePosition)) );
					}

				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		String clientId = entry.getEntryId().getClientId();
		Long msgId = Long.valueOf(entry.getEntryId().getMessageId());
		Integer intWritePosition = Integer.valueOf(writePosition);

		if(!logIndex.containsKey(clientId)){//move to add a client method later
			HashMap<Long, Integer> messages = new HashMap<Long, Integer>();
			messages.put(msgId, intWritePosition);
			logIndex.put(clientId, messages);
		}else

			logIndex.get(clientId).put(msgId, intWritePosition);

		if(persistClient.contains(clientId)){
			persistIndexQueue.offer(Integer.valueOf(writePosition));
			if(persisterWaitForReader)
				synchronized(persistLock){
					persistLock.notifyAll();
				}
		}
		buffer[writePosition] = entry;
		bufferElementRead.set(writePosition, true);
		bufferElementPersist.set(writePosition, true);

		writePosition = ++writePosition % capacity;
		size.incrementAndGet();

		/*		synchronized(empty){
			empty.notifyAll();
		}
		 */
		if(readerWaitForWritter)
			synchronized(readLock){
				readLock.notifyAll();
			}
		//	print("Buffered " +  entry.getEntryId().getMessageId());
	}



	public void readComplete(int index){
		bufferElementRead.set(index, false);
		if(persisterWaitForReader)
			synchronized (persistLock) {
				print("Notify Persister by thread:" +  Thread.currentThread());
				persistLock.notifyAll();
			}
		//		print("Read Complete " +  id.getMessageId());
	}

	public BufferedLogEntry nextToRead() {
		while(bufferElementRead.get(readPosition)==false){
			try {
				synchronized (readLock) {
					//			print1("ReadLock: waiting thread:" +  Thread.currentThread());
					readerWaitForWritter = true;
					readLock.wait();
					//			print1("Not ReadLock: waiting thread:" +  Thread.currentThread());
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		readerWaitForWritter = false;
		BufferedLogEntry blEntry = null;
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
			blEntry = new BufferedLogEntry();
			blEntry.entry = t;
			blEntry.bufferIndex = readPosition;
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
		//	print("ReadNext From Buffer  " +  t.getEntryId().getMessageId());
		return blEntry;
	}



	public void remove(Identifier id){	
		//System.out.println("LogIndex: " + logIndex );
		//System.out.println("[RP: "+readPosition + " WP: " + writePosition + " PQSz: " + 
		//		persistIndexQueue.size() + " Persist Pos: " + persistPosition + " c/s: " + size.get() + "/"+ capacity + " PersistClient "+ persistClient) ; 
		//	System.out.println("Buffer: " + buffer );

	//	print1("Removing start:" +  "PersistBits: " + bufferElementPersist.toString() + " | ReadBits: " + bufferElementRead.toString());
		String clientId = id.getClientId(); 
		long msgId = id.getMessageId();
		Integer position = logIndex.get(clientId).get(Long.valueOf(msgId));
		if(position==null || buffer[position.intValue()]==null)
			try {
				throw new Exception("Position null. Wrong element to remove.ID:"+ id + " LogIndex:" + logIndex );
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
		Object obj;

		obj = logIndex.get(clientId).remove(Long.valueOf(msgId));
		if(obj!=null){
			size.decrementAndGet();
			bufferElementPersist.set(position.intValue(), false);
			persistPosition = position.intValue();
			buffer[position.intValue()]=null;
			print("Removed From Buffer  " + id.getMessageId());
			synchronized (full) {
				full.notifyAll();}
		}
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
		int index = persistIndexQueue.poll().intValue();
		LogEntry entry = buffer[index];
		//bufferElementPersist.set(index, false);
		print("PersistNext From Buffer  " +  entry.getEntryId().getMessageId());
		return entry;
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
		//		System.out.println("BUFFER "+str + "  " + Thread.currentThread().getName() + meta);
	}

	TextFile file = new TextFile("BitSets");
	void print1(String str){
		String meta = "\n[RP: "+readPosition + " WP: " + writePosition + " PQSz: " + 
				persistIndexQueue.size() + " Persist Pos: " + persistPosition + " c/s: " + size.get() + "/"+ capacity + " \n" ; 
		//	file.print(str + meta);
		//	System.out.println("BUFFER "+str + "  "  + meta);
		//System.out.println("Buffer Element Read: " + bufferElementRead);
		//System.out.println("Buffer Element Read: " + bufferElementPersist);
	}

}

