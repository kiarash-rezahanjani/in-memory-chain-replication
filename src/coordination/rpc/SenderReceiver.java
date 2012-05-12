package coordination.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import coordination.AbstractMessage;
import coordination.ProtocolMessage;
import coordination.ReceivedMessageCallBack;
import utility.Configuration;

//later work: catch exceptions and retry mechanism
public class SenderReceiver implements Closeable{
	private ReceivedMessageCallBack callback;
	private UdpClient sender ;
	private UdpServer receiver ;
	private InetSocketAddress serverSocketAddress;
	Configuration config ;

	public SenderReceiver(Configuration config, ReceivedMessageCallBack callback){
		this.config =config;
		this.callback = callback;
		sender = new UdpClient();
		receiver = new UdpServer(config, callback);
		try {
			receiver.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		serverSocketAddress = config.getProtocolSocketAddress();
	}
	
	public InetSocketAddress getServerSocketAddress(){
		return config.getProtocolSocketAddress();
	}

	public void send(InetSocketAddress destination, Object message){
		//for(int i=0; i<2; i++)
		try {
			((ProtocolMessage)message).setSrcSocketAddress(serverSocketAddress);
			//System.out.println("Message sent with rece: "+receiver.getServerSocketAddress());
			sender.sendAsyncMessage(destination, message);
			//		break;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void broadcast(List<InetSocketAddress> destinations, Object message){
		for(InetSocketAddress destination:destinations)
			send(destination, message);
	}

	public void send(String hostAddress, int port, Object message){
		send(new InetSocketAddress(hostAddress, port ), message);
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		sender.stop();
		receiver.stop();
	}

}
