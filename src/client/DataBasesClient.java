package client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import utility.Configuration;
import ensemble.ChainManager;

public class DataBasesClient {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//args=new String[1];
		//args[0]="applicationProperties";
		System.out.println("Config file: " + args[0] +  System.getProperty("user.dir"));
		if(args.length<1){
			System.exit(-1);
		}
		
		DBClient dbcliThread =null;
		try {
			dbcliThread = new DBClient( new Configuration(args[0]) , "localhost", 2111);
			dbcliThread.run();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			dbcliThread.stop();
		//	System.exit(-1);
		}finally{
			
		}
		
	}

	
}
