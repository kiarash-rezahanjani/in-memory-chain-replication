package coordination;

import utility.Configuration;

public class CoordinationTest {

	public static void main(String[] args){
		Configuration conf = new Configuration("applicationProperties");
		Configuration conf1 = new Configuration("applicationProperties1");
		Configuration conf2 = new Configuration("applicationProperties2");
		Configuration conf3 = new Configuration("applicationProperties3");
		Configuration conf4 = new Configuration("applicationProperties4");
		Configuration conf5 = new Configuration("applicationProperties5");
		Configuration conf6 = new Configuration("applicationProperties6");
		Configuration conf7 = new Configuration("applicationProperties7");
		Configuration conf8 = new Configuration("applicationProperties8");
		
		Protocol p = new Protocol(conf, false);
		Protocol p1 = new Protocol(conf1, false);
		Protocol p2 = new Protocol(conf2, true);
		try {
			Thread.sleep(15000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Protocol p3 = new Protocol(conf3, false);
		Protocol p4 = new Protocol(conf4, false);
		Protocol p5 = new Protocol(conf5, false);
		try {
			Thread.sleep(15000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Protocol p6 = new Protocol(conf6, false);
		Protocol p7 = new Protocol(conf7, false);
		Protocol p8 = new Protocol(conf8, false);
	}
}
