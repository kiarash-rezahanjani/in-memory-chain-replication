package persistence;
import utility.TextFile;

import client.Log.LogEntry;
import ensemble.Ensemble;

public class DummyPersister extends AbstractPersister{
	TextFile output = new TextFile("persist/logfile"); ;
	public DummyPersister(Ensemble ensemble) {
		super(ensemble);
	}

	public void close(){
		output.close();
	}
	@Override
	public boolean persistEntry(LogEntry entry) {
		output.print(entry.toString());
		return true;
	}
}
