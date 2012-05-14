package ensemble;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import utility.Configuration;

public class LocalHostReplication {
	public static void main(String[] args) {
		Configuration conf = new Configuration("applicationProperties");
		Configuration conf1 = new Configuration("applicationProperties1");
		Configuration conf2 = new Configuration("applicationProperties2");
		
		ChainManager cm=null;
		ChainManager cm1=null;
		ChainManager cm2=null;
		try {
			cm = new ChainManager(conf);
			cm1 = new ChainManager(conf1);
			cm2 = new ChainManager(conf2);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			cm.close();
			cm1.close();
			cm2.close();
			System.exit(-1);
		}
		
	}
}
