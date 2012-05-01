package JustTesting;

import java.util.List;

public class Persist extends Thread{

	boolean running = true;
	StrList list;
	public Persist(StrList list){
		this.list = list;
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
				Thread.sleep(300);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			list.remove();
		}
		
	}

}
