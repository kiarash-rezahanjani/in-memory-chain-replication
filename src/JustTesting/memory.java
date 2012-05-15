package JustTesting;


public class memory {

//	Sigar sigar = new Sigar();
//	public static Mem memory = ;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int i = 0;
		String str = "";
		for(int j = 0; j<1000; j++){
			str+="aaaaaaaaaaaaaaaaaaaa";
		}
		long size1 = str.length();
		long size2 = str.length(), usedMemory1=0, usedMemory2=0;
		while(i<20){
			usedMemory2 = usedMemory1;
			i++;
			size1 = str.length();
			str+=str;
			size2 = str.length();
			
			Runtime.getRuntime().gc();
			
			usedMemory1 = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
			System.out.println(Runtime.getRuntime().availableProcessors());
			
			System.out.println(Runtime.getRuntime().maxMemory());
			System.out.println(Runtime.getRuntime().totalMemory());
			System.out.println(" newly used memory "+ (usedMemory1-usedMemory2) + " how many times " + (usedMemory1-usedMemory2)/str.length());
			System.out.println( "  str.length "+str.getBytes().length+ " " +str.length() );
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


	}

}
