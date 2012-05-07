package ensemble;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import utility.Configuration;

public class ReplicationServer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//args=new String[1];
		//args[0]="applicationProperties";
		System.out.println("Config file: " + args[0] +  System.getProperty("user.dir"));
		if(args.length<1){
			System.exit(-1);
		}
		
		List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
		addresses.add(new InetSocketAddress("gsbl90152", 2111));
		addresses.add(new InetSocketAddress("gsbl90155", 2112));
		addresses.add(new InetSocketAddress("gsbl90159", 2113));
		ChainManager cm=null;
		try {
			cm = new ChainManager(new Configuration(args[0]));
			cm.newEnsemble(addresses);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			cm.close();
			System.exit(-1);
		}
		
	}

}
