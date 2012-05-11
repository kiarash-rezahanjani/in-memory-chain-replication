package coordination.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

//import org.apache.commons.lang3.time.StopWatch;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;

import coordination.ProtocolMessage;

public final class UdpClient {

	private static ChannelFactory factory;
	private static ConnectionlessBootstrap bootstrap;
	private DatagramChannel channel;
	private InetSocketAddress clientSocketAddress; 
	
	public UdpClient() {
	
		factory = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
		bootstrap = new ConnectionlessBootstrap(factory);
	
		bootstrap.setPipelineFactory(new UdpClientPipelineFactory());
		bootstrap.setOption("reuseAddress", true);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("broadcast", "false");
		bootstrap.setOption("sendBufferSize", 1024);
		bootstrap.setOption("receiveBufferSize", 1024);

		clientSocketAddress = new InetSocketAddress(0);
		channel = (DatagramChannel) bootstrap.bind(clientSocketAddress);
	}

	public void sendAsyncMessage(String hostAddress, int port, final Object message) throws IOException {
		
		sendAsyncMessage(new InetSocketAddress( hostAddress,  port),  message);
		
	}
	
	public void sendAsyncMessage(final InetSocketAddress iNetSocketAddress, final Object message) throws IOException {
	//	System.out.println("sendAsyncMessage "+iNetSocketAddress.toString());
		ChannelFuture future = channel.write(message, iNetSocketAddress);
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		future.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture cf) throws Exception {
			
				if(cf.isSuccess());
			//		System.out.println("Message sent! ID:"+ ((ProtocolMessage) message).getMsgId());
				else
				{
					System.out.println("Message failed! ID:"+ ((ProtocolMessage) message).getMsgId());
					//sendAsyncMessage(iNetSocketAddress,  message); makes error
				}
					//                              cf.getChannel().close();
				//                              factory.releaseExternalResources();
			}                       
		});     
	}
    
	public void stop() {
		channel.close();
		factory.releaseExternalResources();
		bootstrap.releaseExternalResources();
		System.out.println("Client(Port:"+ clientSocketAddress.getPort() +") Stopped!");
		System.exit(0);
	}

}
