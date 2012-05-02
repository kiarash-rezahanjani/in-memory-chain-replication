package JustTesting;

public class Persist extends Thread{

	boolean running = true;
	NaiveCircularBuffer buffer;
	public Persist(NaiveCircularBuffer buffer){
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
				Thread.sleep(300);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//buffer.remove();
		}
		
	}

}
