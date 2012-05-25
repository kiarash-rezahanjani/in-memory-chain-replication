package persistence;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Inet4Address;
import java.net.UnknownHostException;

import utility.TextFile;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;
import ensemble.Buffer;
import ensemble.Ensemble;

public class DummyPersister extends AbstractPersister{
	TextFile output = new TextFile("PersistedLogs"); ;
	public DummyPersister(Ensemble ensemble) {
		super(ensemble);
	}

	public void close(){
		output.close();
	}
	@Override
	public boolean persistEntry(LogEntry entry) {
		output.print(entry.getEntryId().toString());
		return true;
	}
}
