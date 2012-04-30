package ensemble;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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

public class NaiveCircularBuffer implements CircularBuffer{

	final private LogEntry[] buffer;
	private int readPosition = 0;
	private int writePosition = 0;
	private int persistPosition = 0;
	final private int capacity ;
	private AtomicInteger size = new AtomicInteger(); 
	private HashMap<String, HashMap<Long, Integer>> logIndex = new HashMap<String, HashMap<Long, Integer>>();//<clientId,<msgId, poistion in the buffer>>
	Queue<Integer> persistIndexQueue = new LinkedList<Integer>();
	public List<String> persistClient = new ArrayList<String>();//should be synchronized
	private BitSet bufferElementStat; //jus to check: each bit at position x indicates whether the element at this position is persisted
	private Semaphore addSem ;
	private Semaphore removeSem ;
	
	public NaiveCircularBuffer(int capacity) {
		this.capacity = capacity;
		addSem = new Semaphore(capacity);
		removeSem = new Semaphore(capacity);
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
/*		while(size.get()>=buffer.length){
			try {
				wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	*/	
		try {
			addSem.acquire();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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

		if (size.get()<buffer.length) {
			buffer[writePosition++] = entry;
			bufferElementStat.set(writePosition, true);
			if(persistClient.contains(clientId))
				persistIndexQueue.offer(Integer.valueOf(writePosition));
			
			size.incrementAndGet();
		} else {

			//we can say wait till its available
			throw new BufferOverflowException();
		}
		writePosition = writePosition % buffer.length;
		
//		if(size.get()==1 || readPosition+2 == writePosition || persistIndexQueue.size()==1)

	}

	public LogEntry nextToRead() {
/*		while(size.get()<=0 || (readPosition+1)==writePosition){
			try {
				wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		*/
		LogEntry t = null;
		if (size.get()>0 && (readPosition+1)!=writePosition) {
			t = (LogEntry) buffer[readPosition++];
			readPosition = readPosition % buffer.length;
		} else {
			throw new BufferUnderflowException();
		}
		return t;
	}
	
	public void remove(Identifier id){
/*		while(size.get()<=0){
			try {
				wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			};
		}
*/
		String clientId = id.getClientId(); 
		long msgId = id.getMessageId();
		int position = logIndex.get(clientId).get(Long.valueOf(msgId));
		Object obj = logIndex.get(clientId).remove(Long.valueOf(msgId));
		
		if(bufferElementStat.get(position)==false)
			try {
				throw new Exception("Remove element which doesnt exist. bufferElementStat.get(position)==false.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		bufferElementStat.set(position, false);
		
		if(obj!=null)
			size.decrementAndGet();
		
		
		
	//	if(size.get()==capacity-1)
//			notifyAll();
	}
	
	@Override
	public LogEntry nextToPersist() {
		// TODO Auto-generated method stub
/*		while(persistIndexQueue.size()<=0){
			try {
				wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
*/		return buffer[persistIndexQueue.poll().intValue()];
	}

	public String toString() {
		return "CircularBuffer(capacity=" + buffer.length + 
				", write=" + writePosition + ", read=" + readPosition + 
				" Size=" + size.get() + 
				" HASH: " + logIndex.toString();
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
