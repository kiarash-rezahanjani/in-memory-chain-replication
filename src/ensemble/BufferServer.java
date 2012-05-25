package ensemble;

import utility.Configuration;

public class BufferServer {

	public static void main(String[] args) {
		System.out.println("Config file: " + args[0] + System.getProperty("user.dir"));
		if(args.length<1){
		System.exit(-1);
		}
		Configuration conf = new Configuration(args[0]);
		ChainManager cm=null;
		try {
			cm = new ChainManager(conf);
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			cm.close();
		}
	}
}