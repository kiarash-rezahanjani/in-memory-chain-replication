package JustTesting;
public class Add extends Thread{

	boolean running = true;
	ConcurrentCircularBuffer buffer;
	int time ;
	public Add(ConcurrentCircularBuffer buffer, int time){
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
		for (int i = 0; i < 50000 && running; i++) 
		{
			LogEntry entry = new LogEntry(new Identifier("cli", i));
			try {
				buffer.add(entry);
				Thread.sleep(time);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


		}

		
	}

}
