package JustTesting;

import java.util.List;

public class Read extends Thread{

	boolean running = true;
	StrList list;
	public Read(StrList list){
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
				Thread.sleep(600);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			list.add();
		}
		
	}

}
