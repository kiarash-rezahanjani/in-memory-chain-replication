package coordination;

import java.net.InetSocketAddress;

import coordination.InterProcessCoordinator;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		InterProcessCoordinator co1 = new InterProcessCoordinator();
		InterProcessCoordinator co2 = new InterProcessCoordinator();
		
		//Protocol p1 = new Protocol(co1, 1111, false);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Protocol p2 = new Protocol(co2, 2222, true);
		 
		
		
	}

}
