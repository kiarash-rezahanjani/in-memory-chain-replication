package streaming;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import client.DBClient;
import client.Log.LogEntry;
import ensemble.ChainManager;
import ensemble.CircularBuffer;
import ensemble.Ensemble;
import ensemble.NaiveCircularBuffer;

import utility.Configuration;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
public class CliSerTest {

	public static void main(String[] args){
		//Test1();
		Test3();
	}

	//test client and ensembles
	private static void Test3() {


		List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
		addresses.add(new InetSocketAddress("localhost", 2111));
		addresses.add(new InetSocketAddress("localhost", 2112));
		addresses.add(new InetSocketAddress("localhost", 2113));

		Configuration conf = new Configuration();
		Configuration conf1 = new Configuration("applicationProperties1");
		Configuration conf2 = new Configuration("applicationProperties2");

		//Ensemble ensemble;

		//ensemble = new Ensemble(conf, addresses);

		ChainManager cm = null;
		ChainManager cm1 =  null;
		ChainManager cm2 =  null;
		Thread dbcliThread;
		Thread dbcliThread1;
		try{
			cm = new ChainManager(conf);
			cm1 = new ChainManager(conf1);
			cm2 = new ChainManager(conf2);
			Thread.sleep(1000);

			boolean bcm = cm.newEnsemble(addresses);
			boolean bcm1 = cm1.newEnsemble(addresses);
			boolean bcm2 = cm2.newEnsemble(addresses);
			
			System.out.println(bcm+ "  " +bcm1+ "   " + bcm2);
			
		//	System.out.println(cm.ensemble.getPredecessor() + " <->" + cm.ensemble.getSuccessor());
		//	System.out.println(cm1.ensemble.getPredecessor() + "<->" + cm1.ensemble.getSuccessor());
		//	System.out.println(cm2.ensemble.getPredecessor() + "<->" + cm2.ensemble.getSuccessor());
			
			
			
			dbcliThread = new Thread( new DBClient(new Configuration("applicationProperties3"), "localhost", 2111));
			dbcliThread.start();

			dbcliThread = new Thread( new DBClient(new Configuration("applicationProperties4"), "localhost", 2112));
			dbcliThread.start();
			
			dbcliThread.join();


		}catch(Exception e)
		{
			e.printStackTrace();
		}finally{
		
			cm.close();
			cm1.close();
			cm2.close();
		}
	}

	//test only the ensemble servers
	private static void Test2() {
		try{
			List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
			addresses.add(new InetSocketAddress("localhost", 1111));
			addresses.add(new InetSocketAddress("localhost", 1112));
			addresses.add(new InetSocketAddress("localhost", 1113));

			Configuration conf = new Configuration();
			Configuration conf1 = new Configuration("applicationProperties1");
			Configuration conf2 = new Configuration("applicationProperties2");

			//Ensemble ensemble;

			//ensemble = new Ensemble(conf, addresses);

			ChainManager cm = new ChainManager(conf);
			ChainManager cm1 = new ChainManager(conf1);
			ChainManager cm2 = new ChainManager(conf2);

			Thread.sleep(1000);

			boolean bcm = cm.newEnsemble(addresses);
			boolean bcm1 = cm1.newEnsemble(addresses);
			boolean bcm2 = cm2.newEnsemble(addresses);
			System.out.println(cm.ensemble.getPredecessor() + " <->" + cm.ensemble.getSuccessor());
			System.out.println(cm1.ensemble.getPredecessor() + "<->" + cm1.ensemble.getSuccessor());
			System.out.println(cm2.ensemble.getPredecessor() + "<->" + cm2.ensemble.getSuccessor());
			cm.close();
			cm1.close();
			cm2.close();
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	/*
	private static void Test1() {
		try{
			List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
			addresses.add(new InetSocketAddress("localhost", 1111));
			addresses.add(new InetSocketAddress("localhost", 1112));
			addresses.add(new InetSocketAddress("localhost", 1113));
			Configuration conf = new Configuration();
			Ensemble ensemble;

			ensemble = new Ensemble(conf, addresses);

			BufferClient client = new BufferClient(ensemble);
			BufferServer server = new BufferServer(conf, ensemble);
			Channel channel;

			channel = client.connectTo(conf.getBufferServerSocketAddress());
			ChannelFuture future=null;
			for (int i = 0; i < 10; i++) 
			{
				LogEntry entry = LogEntry.newBuilder()
						.setClientId("ali")
						.setMessageId(i)
						.setKey("Bale")
						.setClientSocketAddress("localhost:2222")
						.setOperation("add key o").build();
				System.out.print("start sending..");
				future = channel.write(entry);
			}
			if(future.await().isDone())
				channel.close();
			else
				System.out.print("NOT DONE");
			server.stop();
			client.stop();
			//channel.close();
		}catch(Exception e)
		{e.printStackTrace(); 
		//	server.stop();
		//	client.stop();
		//channel.close();
		System.exit(-1);}
	}
	 */
}
