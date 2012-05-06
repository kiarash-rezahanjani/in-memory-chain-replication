package ensemble;

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

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;

/**
 * Only one thread uses add and one thread uses remove method and one thread uses read.
 * One assumption is that before persist operation the entry has certainly been read 
 * meaning that its been replicated and its Ack has been send. 
 * @author root
 *
 */

public class NaiveCircularBuffer {

	final private LogEntry[] buffer;
	private int readPosition = 0;
	private int writePosition = 0;
	private int persistPosition = 0;
	final private int capacity ;
	private AtomicInteger size = new AtomicInteger(); 
	private HashMap<String, HashMap<Long, Integer>> logIndex = new HashMap<String, HashMap<Long, Integer>>();//<clientId,<msgId, poistion in the buffer>>
	Queue<Integer> persistIndexQueue = new LinkedList<Integer>();
	private Set<String> persistClient = new HashSet<String>();//should be synchronized
	private BitSet bufferElementStat; //jus to check: each bit at position x indicates whether the element at this position is persisted
	Object full = new Object();
	Object empty = new Object();
	Object readLock = new Object();
	Object persistLock = new Object();
	boolean persisterWaitForReader = false;

	//	private Semaphore addSem ;
	//	private Semaphore removeSem ;

	public NaiveCircularBuffer(int capacity, Set<String> persistClientIdSet) {
		this.capacity = capacity;
		this.persistClient = persistClientIdSet;
		//		addSem = new Semaphore(capacity);
		//		removeSem = new Semaphore(capacity);
		bufferElementStat = new BitSet(this.capacity);
		buffer = new LogEntry[capacity];
		readPosition = 0;
		writePosition = 0;
		size.set(0);
	}

	public void add(Object obj) {
		add((LogEntry)obj);
	}

	public void add(LogEntry entry) {

		while(size.get()>=capacity){
			synchronized(full){
				try {
					print("full: waiting thread:" +  Thread.currentThread());
					full.wait();
					print("Not full: waiting thread:" +  Thread.currentThread());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		String clientId = entry.getEntryId().getClientId();
		Long msgId = Long.valueOf(entry.getEntryId().getMessageId());
		Integer position = Integer.valueOf(writePosition);

		if(!logIndex.containsKey(clientId)){
			HashMap<Long,Integer> messages = new HashMap<Long, Integer>();
			messages.put(msgId, position);
			logIndex.put(clientId, messages);
		}else
			logIndex.get(clientId).put(msgId, position);

		if (size.get()<capacity) {
			bufferElementStat.set(writePosition, true);
			if(persistClient.contains(clientId)){
				persistIndexQueue.offer(Integer.valueOf(writePosition));
				//if(persistIndexQueue.size()==1)
				{
					synchronized(persistLock){
						persistLock.notifyAll();
					}
				}
			}
			buffer[writePosition++] = entry;
			writePosition = writePosition % capacity;
			size.incrementAndGet();
		} else {

			//we can say wait till its available
			throw new BufferOverflowException();
		}
		writePosition = writePosition % buffer.length;
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

	public LogEntry nextToRead() {
		/**
		 * readP==writeP start when position are both at 0
		 * readP+1==writeP when nothing is left to read
		 */
		//while((readPosition+1)==writePosition || readPosition==writePosition){
		while(bufferElementStat.get(readPosition)==false){
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
		//	if (size.get()>0 && readPosition!=writePosition) {
		if(bufferElementStat.get(readPosition)==false)
			try {
				throw new Exception("Read element which doesnt exist. bufferElementStat.get(position)==false.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		t = (LogEntry) buffer[readPosition++];
		bufferElementStat.set(readPosition, false);
		readPosition = readPosition % capacity;
		if(persisterWaitForReader == true)
			synchronized (persistLock) {
				print("Notify Persister by thread:" +  Thread.currentThread());
				persistLock.notifyAll();
			}

		/*		} else {
			try {
				throw new Exception(String.valueOf(size.get()> 0) + String.valueOf(readPosition!=writePosition));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		 */

		//bufferElementStat.set(readPosition, false);
		print("Read " +  Thread.currentThread());
		return t;
	}

	public void remove(Identifier id){
		//this condition should never happens as the id correspondent entry must be in the buffer
		while(size.get()<=0){
			synchronized(empty){
				try {
					print("empty: waiting thread:" +  Thread.currentThread());
					empty.wait();
					print("empty: waiting thread:" +  Thread.currentThread());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		if(!id.hasClientId() || !id.hasMessageId())
			try {
				throw new Exception("Id is Null");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.exit(-1);
			}

		String clientId = id.getClientId(); 
		long msgId = id.getMessageId();
		int position = logIndex.get(clientId).get(Long.valueOf(msgId));
		Object obj = logIndex.get(clientId).remove(Long.valueOf(msgId));

		if(bufferElementStat.get(position)==true)
			try {
				throw new Exception("Trying to remove an element which is not read yet. Element ID: "+ id);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


		if(obj!=null)
			size.decrementAndGet();


		//if(size.get()==capacity-1)
		synchronized (full) {
			full.notifyAll();}
		print("Removed " +  Thread.currentThread());
		//	if(size.get()==capacity-1)
		//			notifyAll();
	}

	boolean isRead(){
		return false;
	}

//	@Override
	public LogEntry nextToPersist() {
		// TODO Auto-generated method stub

		while(persistIndexQueue.size()<=0 || bufferElementStat.get(persistIndexQueue.peek())){
			synchronized(persistLock){
				try {
					print("persistlock: waiting thread:" +  Thread.currentThread());
					persisterWaitForReader = true;
					persistLock.wait();
					print("persistlock: waiting thread:" +  Thread.currentThread());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}	
		persisterWaitForReader = false;
		print("Persist " +  Thread.currentThread());
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
		//System.out.println("BUFFER "+str + "  " + Thread.currentThread().getName() + meta);
	}

//	@Override
	public void readComplete(Identifier id) {
		// TODO Auto-generated method stub
		
	}

	/*
	public static void main(String[] args) {

		NaiveCircularBuffer  b = new NaiveCircularBuffer (5);
		int removeIndx = 0;
		for (int i = 0; i < 20; i++) 
		{
			LogEntry entry = LogEntry.newBuilder()
					.setClientId("ali")
					.setMessageId(i)
					.setKey("Bale")
					.setClientSocketAddress("localhost:2222")
					.setOperation("add key o").build();
			b.add(entry);
			System.out.println(b);
			if(i%2==1)
			{
				b.get();
				b.remove(new Identifier(){

					@Override
					public String getClientId() {
						// TODO Auto-generated method stub
						return "ali";
					}

					@Override
					public long getMessageId() {
						// TODO Auto-generated method stub
						return 1;
					}

				});
				//System.out.println(b);
			}

		}
	}
	 */


}
