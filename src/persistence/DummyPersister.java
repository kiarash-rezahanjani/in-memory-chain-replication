package persistence;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;
import ensemble.Buffer;
import ensemble.Ensemble;

public class DummyPersister extends AbstractPersister{
	FileOutputStream out;
	PrintStream persist;
	public DummyPersister(Ensemble ensemble) {
		super(ensemble);
		// TODO Auto-generated constructor stub
		try {
			out = new FileOutputStream("persistedlogs");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		persist = new PrintStream(out);
	}

	@Override
	public boolean persistEntry(LogEntry entry) {
		persist.append(entry.getEntryId().toString());
		return true;
	}




}
