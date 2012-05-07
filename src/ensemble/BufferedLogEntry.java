package ensemble;

import client.Log.LogEntry;

public class BufferedLogEntry{
	public LogEntry entry;
	public int bufferIndex;
	public BufferedLogEntry(LogEntry entry, int bufferIndex){
		this.entry = entry;
		this.bufferIndex = bufferIndex;
	}
	public BufferedLogEntry(){

	}
}