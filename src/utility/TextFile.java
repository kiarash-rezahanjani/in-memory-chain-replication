package utility;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;

import client.Log.LogEntry;

public class TextFile implements Closeable {
	//for testing 
	FileOutputStream out;
	PrintStream persist;

	public TextFile(String file){
		try {
			out = new FileOutputStream(file, true);
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
		persist.append(str+"\n");
	}
	public void close(){
		try {
			persist.flush();
			out.flush();
			persist.close();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
