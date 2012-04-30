package ensemble;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import client.Log.LogEntry;
import streaming.BufferServerException;
import streaming.BufferServerException.BufferMaxCapacityException;
import streaming.BufferServerException.BufferOverflowException;
/**
 * circular buffer
 * All the elements are indexed using client id and message id with their respective size and position in the buffer.
 * @author root
 *
 */
public class LogBuffer {

	public static final int KB = 1024;
	public static final int DEFAULT_CAPACITY = 64 * 1024 ;//in KB
	public static final int MAX_CAPACITY = Integer.MAX_VALUE/KB;//in KB
	private static int capacity ;
	private byte[] buffer;
	private int readPosition = 0;
	private int writePosition = 0;
	private AtomicInteger filledBuffer ; 
	//add a queue to hold the indexed in order of insertion
	/**
	 * <clinetId, <messageId, Pair(position,length)> >
	 */
	private HashMap<String, HashMap<Long, Pair>> logIndex = new HashMap<String, HashMap<Long, Pair>>();
	
	class Pair{
		final int position;
		final int length;
		public int getPosition() {
			return position;
		}
		public int getLength() {
			return length;
		}
		public Pair(int position, int length){
			this.position = position;
			this.length = length;
		}
	}
	
	/**
	 * 
	 * @param capacity capacity in KB
	 * @throws BufferMaxCapacityException 
	 */
	
	public LogBuffer(int capacity) throws BufferMaxCapacityException{
		if(capacity > MAX_CAPACITY)
			throw new BufferMaxCapacityException();
		this.capacity = capacity * KB;
		 filledBuffer.set(0);
		buffer = new byte[this.capacity];
	}
	
	public int write(LogEntry log) throws BufferOverflowException{
		byte[] serializedLog = new byte[10];
		int length = log.getSerializedSize();
		if(length> capacity-filledBuffer.get())
			throw new BufferOverflowException();
		
		Long msgId = Long.valueOf(log.getEntryId().getMessageId());
		Pair posLen = new Pair(writePosition,length);
		String clientId = log.getEntryId().getClientId();
		
		if(!logIndex.containsKey(clientId)){
			HashMap<Long,Pair> messages = new HashMap<Long, LogBuffer.Pair>();
			messages.put(msgId, posLen);
			logIndex.put(clientId, messages);
		}else
			logIndex.get(clientId).put(msgId, posLen);

		if(writePosition + length <= capacity){ 
			System.arraycopy(serializedLog, 0, buffer, writePosition, length);
			writePosition = add(writePosition, length);
			}
		else{
			int tailSize = capacity - writePosition;//left bytes size at the tails of buffer
			System.arraycopy(serializedLog, 0, buffer, writePosition, tailSize);
			writePosition = add(writePosition, tailSize);
		
			System.arraycopy(serializedLog, tailSize, buffer, writePosition, length-tailSize);
			writePosition = add(writePosition, length-tailSize);
		}
	//	 filledBuffer.addAndGet(length)	;
			
		return writePosition; 
	}

	public int read(){
		int length = 0;
		
		return add(readPosition, length);
	}
	
	int add(int position, int length){
		return (position+length)%capacity;
	}
	

}
