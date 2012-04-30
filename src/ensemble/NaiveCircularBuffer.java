package ensemble;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;



public class NaiveCircularBuffer implements CircularBuffer{

	private LogEntry[] buffer;
	private int readPosition = 0;
	private int writePosition = 0;
	private AtomicInteger size = new AtomicInteger(); 
	private HashMap<String, HashMap<Long, Integer>> logIndex = new HashMap<String, HashMap<Long, Integer>>();
	
	public NaiveCircularBuffer(int capacity) {
		buffer = new LogEntry[capacity];
		 readPosition = 0;
		 writePosition = 0;
		 size.set(0);
	}

	public void add(Object obj) {
		add((LogEntry)obj);
	}
	
	public void add(LogEntry entry) {
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
			size.incrementAndGet();
		} else {
			//we can say wait till its available
			throw new BufferOverflowException();
		}
		writePosition = writePosition % buffer.length;
	}

	public LogEntry get() {
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
		String clientId = id.getClientId(); 
		long msgId = id.getMessageId();
		
		Object obj = logIndex.get(clientId).remove(msgId);
		if(obj!=null)
			size.decrementAndGet();
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
