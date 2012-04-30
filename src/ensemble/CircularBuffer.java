package ensemble;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;

public interface CircularBuffer {

	public void add(Object object);
	public void add(LogEntry entry);
	public Object nextToRead();
	public LogEntry nextToPersist();
	public void remove(Identifier id);
}
