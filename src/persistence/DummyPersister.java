package persistence;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;
import ensemble.CircularBuffer;
import ensemble.Ensemble;

public class DummyPersister extends AbstractPersister{

	public DummyPersister(Ensemble ensemble) {
		super(ensemble);
		// TODO Auto-generated constructor stub
	}

	@Override
	public LogEntry persist(LogEntry entry) {
		// TODO Auto-generated method stub
		return getPersistedMessage(entry);
	}
	



}
