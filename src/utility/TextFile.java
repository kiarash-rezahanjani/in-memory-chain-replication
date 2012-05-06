package utility;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import client.Log.LogEntry;

public class TextFile {
	//for testing 
	FileOutputStream out;
	PrintStream persist;

	public TextFile(String file){
		try {
			out = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		persist = new PrintStream(out);
	}

	public void print(LogEntry entry) {
		persist.append(entry.toString());
	}
	public void print(String str) {
		persist.append(str);
	}
	
}
