package JustTesting;

import java.util.buffer;

public class Read extends Thread{
	boolean running = true;
	NaiveCircularBuffer buffer;
	public Read(NaiveCircularBuffer buffer){
		this.buffer = buffer;
	}
	public void stopThread(){
		running = false;
		interrupt();
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		
		while(running){
			try {
				Thread.sleep(600);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		//	buffer.add();
		}
		
	}

}
