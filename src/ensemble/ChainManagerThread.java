package ensemble;

import java.net.InetSocketAddress;
import java.util.List;

import javax.management.RuntimeErrorException;

import utility.Configuration;

public class ChainManagerThread extends Thread{

	ChainManager cm;
	List<InetSocketAddress> addresses;
	boolean running = true;
	public ChainManagerThread(Configuration conf, List<InetSocketAddress> addresses){
		try {
			cm = new ChainManager(conf);
			this.addresses = addresses;			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void stopRunning(){
		running = false;
		interrupt();
		cm.close();
	}

	public void run(){
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			cm.newEnsemble(addresses);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
}
