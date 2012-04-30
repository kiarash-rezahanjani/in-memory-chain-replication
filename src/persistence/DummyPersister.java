package persistence;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import ensemble.CircularBuffer;

public class DummyPersister extends AbstractPersister{

	public DummyPersister(CircularBuffer buffer) {
		super(buffer);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Identifier persist(LogEntry entry) {
		// TODO Auto-generated method stub
		return entry.getEntryId();
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			try {
				
				LogEntry entry = buffer.nextToPersist();
				Thread.sleep(1);//persist
				buffer.remove(entry.getEntryId());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
