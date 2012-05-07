package JustTesting;


public class Read extends Thread{
	boolean running = true;
	HashedBuffer buffer;
	int time;
	public Read(HashedBuffer buffer, int time){
		this.buffer = buffer;
		this.time = time;
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
				LogEntry entry = buffer.nextToRead();
				Thread.sleep(time);
				buffer.readComplete(entry.getEntryId());
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		//	buffer.add();
		}
		
	}

}
