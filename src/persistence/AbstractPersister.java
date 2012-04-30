package persistence;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;
import ensemble.CircularBuffer;
import ensemble.Ensemble;


public abstract class AbstractPersister implements Runnable {

	Ensemble ensemble;
	public AbstractPersister(Ensemble ensemble){
		this.ensemble=ensemble;
	}
	public abstract LogEntry persist(LogEntry entry);
	
	public LogEntry getPersistedMessage(LogEntry entry){
		return LogEntry.newBuilder().setMessageType(Type.ENTRY_PERSISTED)
		.setEntryId(entry.getEntryId()).build();
	}
	
	
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			try {
				
				LogEntry entry = ensemble.getBuffer().nextToPersist();
			
				Thread.sleep(1);//persist
				LogEntry persistedMessage = persist(entry);
				System.out.println("Persisted: " + persistedMessage);
				
				try {
					ensemble.entryPersisted(persistedMessage);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
