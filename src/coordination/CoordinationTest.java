package coordination;

import utility.Configuration;

public class CoordinationTest {

	public static void main(String[] args){
		Configuration conf = new Configuration("applicationProperties");
		Configuration conf1 = new Configuration("applicationProperties1");
		Configuration conf2 = new Configuration("applicationProperties2");
		Protocol p = new Protocol(conf, false);
		Protocol p1 = new Protocol(conf1, false);
		Protocol p2 = new Protocol(conf2, true);
	}
}
