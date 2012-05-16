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
		
		Configuration conf3 = new Configuration("applicationProperties3");
		Configuration conf4 = new Configuration("applicationProperties4");
		Configuration conf5 = new Configuration("applicationProperties5");
		
		Configuration conf6 = new Configuration("applicationProperties6");
		Configuration conf7 = new Configuration("applicationProperties7");
		Configuration conf8 = new Configuration("applicationProperties8");
		
		Configuration conf9 = new Configuration("applicationProperties9");
		Configuration conf10 = new Configuration("applicationProperties10");
		Configuration conf11 = new Configuration("applicationProperties11");
		
		ChainManager cm=null;
		ChainManager cm1=null;
		ChainManager cm2=null;
		
		ChainManager cm3=null;
		ChainManager cm4=null;
		ChainManager cm5=null;
		
		ChainManager cm6=null;
		ChainManager cm7=null;
		ChainManager cm8=null;
		
		ChainManager cm9=null;
		ChainManager cm10=null;
		ChainManager cm11=null;

		try {
			cm = new ChainManager(conf);
			cm1 = new ChainManager(conf1);
			cm2 = new ChainManager(conf2);
			
			Thread.sleep(2000);
			
			cm3 = new ChainManager(conf3);
			cm4 = new ChainManager(conf4);
			cm5 = new ChainManager(conf5);
			
			Thread.sleep(2000);
			
			cm6 = new ChainManager(conf6);
			cm7 = new ChainManager(conf7);
			cm8 = new ChainManager(conf8);
			
			Thread.sleep(2000);
			
			cm9 = new ChainManager(conf9);
			cm10 = new ChainManager(conf10);
			cm11 = new ChainManager(conf11);
			
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
