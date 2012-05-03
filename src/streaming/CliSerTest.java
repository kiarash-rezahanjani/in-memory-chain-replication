package streaming;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;

import client.DBClient;
import client.IDGenerator;
import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import ensemble.ChainManager;
import ensemble.ChainManagerThread;
import ensemble.CircularBuffer;
import ensemble.ClientServerCallback;
import ensemble.Ensemble;
import ensemble.NaiveCircularBuffer;

import utility.Configuration;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
public class CliSerTest {

	public static void main(String[] args){
		//Test1();
		Test5();
	}
	private static void Test5() {
		ClientServerCallback calback = new ClientServerCallback() {

			@Override
			public void serverReceivedMessage(MessageEvent e) {
				// TODO Auto-generated method stub
				System.out.println("server got  " + ((LogEntry)e.getMessage()).getEntryId());
				if(((LogEntry)e.getMessage()).getEntryId().getMessageId()==2)
					e.getChannel().disconnect();
			}

			@Override
			public void serverAcceptedConnection(ChannelStateEvent e) {
				// TODO Auto-generated method stub

			}

			@Override
			public void exceptionCaught(ExceptionEvent e) {
				// TODO Auto-generated method stub
				if (e.getChannel().isConnected()) {
					System.out.println("\n\nClosing the channel " +  e.getChannel() + " by "  );
					e.getChannel().write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
				}
			}

			@Override
			public void clientReceivedMessage(MessageEvent e) {
				// TODO Auto-generated method stub

			}

			@Override
			public void channelClosed(ChannelStateEvent e) {
				// TODO Auto-generated method stub
				if (e.getChannel().isConnected()) {
					System.out.println("\n\nClosing the channel " +  e.getChannel() + " by "  );
		//			e.getChannel().write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
				}

			}
		};
		Configuration conf = new Configuration();
		BufferServer server = new BufferServer(conf, calback);
		BufferClient client = new BufferClient(conf, calback);

		Channel ch = client.connectServerToServer(conf.getBufferServerSocketAddress());
		try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for(int i = 0 ; i<10; i++){
			
			Identifier id = Identifier.newBuilder()
					.setClientId(conf.getDbClientId())
					.setMessageId(1).build();

			LogEntry entry = LogEntry.newBuilder()
					.setEntryId(Identifier.newBuilder().setClientId("clie1").setMessageId(i).build())
					.setKey("Key")
					.setClientSocketAddress(conf.getBufferServerSocketAddress().toString())
					.setOperation("Opt.add(pfffff)").build();

			ch.write(entry ).addListener(new Listener(entry.getEntryId().getMessageId()));
		}

	}
	public static class Listener implements ChannelFutureListener{

		public long id ;
		Listener(long id){
			this.id = id ;
		}
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			// TODO Auto-generated method stub
			if(future.isSuccess()){
				System.out.println("Success for " + id);
			}
		}
		
	} 
	
	private static void Test4() {


		List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
		addresses.add(new InetSocketAddress("localhost", 2111));
		addresses.add(new InetSocketAddress("localhost", 2112));
		addresses.add(new InetSocketAddress("localhost", 2113));

		Configuration conf = new Configuration();
		Configuration conf1 = new Configuration("applicationProperties1");
		Configuration conf2 = new Configuration("applicationProperties2");
		Configuration conf3 = new Configuration("applicationProperties3");

		//Ensemble ensemble;
		ChainManagerThread cm = null;
		ChainManagerThread cm1 =  null;
		ChainManagerThread cm2 =  null;

		//client
		DBClient dbcliThread = null;
		//DBClient dbcliThread1;
		try{
			cm = new ChainManagerThread(conf,addresses);
			cm.start();
			cm1 = new ChainManagerThread(conf1,addresses);
			cm1.start();
			cm2 = new ChainManagerThread(conf2,addresses);
			cm2.start();

			Thread.sleep(2000);
			dbcliThread = new DBClient(conf3 , "localhost", 2111);
			dbcliThread.start();

		}catch(Exception e)
		{
			e.printStackTrace();
		}finally{

			dbcliThread.stopRunning();
			cm.stopRunning();
			cm1.stopRunning();
			cm2.stopRunning();
		}
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

			//		dbcliThread1 = new Thread( new DBClient(new Configuration("applicationProperties4"), "localhost", 2112));
			//		dbcliThread1.start();

			dbcliThread.join();
			//	dbcliThread1.join();


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
		ChainManager cm = null;
		ChainManager cm1 = null;
		ChainManager cm2 = null;
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

			cm = new ChainManager(conf);
			cm1 = new ChainManager(conf1);
			cm2 = new ChainManager(conf2);

			Thread.sleep(1000);

			boolean bcm = cm.newEnsemble(addresses);
			boolean bcm1 = cm1.newEnsemble(addresses);
			boolean bcm2 = cm2.newEnsemble(addresses);
			System.out.println(cm.ensemble.getPredecessorChannel() + " <->" + cm.ensemble.getSuccessorChannel());
			System.out.println(cm1.ensemble.getPredecessorChannel()+ "<->" + cm1.ensemble.getSuccessorChannel());
			System.out.println(cm2.ensemble.getPredecessorChannel() + "<->" + cm2.ensemble.getSuccessorChannel());

		}catch(Exception e)
		{
			e.printStackTrace();
		}finally{
			cm.close();
			cm1.close();
			cm2.close();
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
