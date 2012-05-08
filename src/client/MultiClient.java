package client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import utility.Configuration;
import ensemble.ChainManager;

public class MultiClient {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//args=new String[1];
		//args[0]="applicationProperties";
		System.out.println("Config file: " + args[0] +  System.getProperty("user.dir"));
		if(args.length<1){
			System.exit(-1);
		}
		DBClient[] dbcliThread = new DBClient[args.length];
		for(int i=0; i<args.length ; i++){
			
			try {
				dbcliThread[i] = new DBClient( new Configuration(args[i]) , "gsbl90152", 2111);
				dbcliThread[i].run();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				dbcliThread[i].stop();
				//	System.exit(-1);
			}finally{

			}
		}
	}


}
