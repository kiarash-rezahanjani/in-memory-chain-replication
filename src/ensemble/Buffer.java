package ensemble;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;

public interface Buffer {

	public void add(Object object);
	public void add(LogEntry entry);

	/**
	 * Returns the next element in t he buffer that should be send to next 
	 * buffer server or should be Acknowledged to client.
	 * @return
	 */
	public LogEntry nextToRead();
	
	/**
	 * 
	 * @param id
	 */
	public void readComplete(Identifier id);
	/**
	 * Return the next element in the buffer that has to be persisted by this 
	 * buffer server. This method does not remove the entry from the buffer
	 * however it removes the index of the entry from the list of entries to be persisted.
	 * @return
	 */
	public LogEntry nextToPersist();
	
	/**
	 * Remove the entry and associated meta data from the buffer.
	 * @param id
	 */
	public void remove(Identifier id);
	
	/**
	 * Remove all entries with bigger messageId(for the given client) contained in the Identifier.
	 * @param lastAckedId : contains the cliend ID and messageID
	 */
	public void garbageCollect(Identifier lastAckedId);
	
	/**
	 * this method remove the elements from the read queue and add to persist queue
	 * this method should be called only when the ensemble is broken so that all the buffered logs will be persisted.
	 * 
	 */
	public void fillPersistQueue();
}
