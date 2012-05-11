package coordination;

import java.net.InetSocketAddress;

import coordination.InterProcessCoordinator;

public class TestProtocol {

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		InterProcessCoordinator co2 = new InterProcessCoordinator("applicationProperties1");
		InterProcessCoordinator co3 = new InterProcessCoordinator("applicationProperties2");
		InterProcessCoordinator co4 = new InterProcessCoordinator("applicationProperties3");
		InterProcessCoordinator co1 = new InterProcessCoordinator("applicationProperties");
		/*
		System.out.println("PROTOCOL 4.");
		
		Protocol p4 = new Protocol(co4, 4444, false);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.out.println("PROTOCOL 3.");
		
		Protocol p3 = new Protocol(co3, 3333, false);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.out.println("PROTOCOL 1.");
		
		Protocol p1 = new Protocol(co1, 1111, false);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("PROTOCOL 2.");
		
		Protocol p2 = new Protocol(co2, 2222, true);
		p2.leaderStartsFormingEnsemble(4);
		*/
	}
	}