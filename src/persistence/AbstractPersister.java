package persistence;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import ensemble.CircularBuffer;


public abstract class AbstractPersister implements Runnable {

	CircularBuffer buffer;
	public AbstractPersister(CircularBuffer buffer){
		this.buffer = buffer;
	}
	public abstract Identifier persist(LogEntry entry);
}
