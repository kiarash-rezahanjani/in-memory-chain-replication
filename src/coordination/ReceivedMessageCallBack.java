package coordination;

import java.net.InetSocketAddress;

import org.jboss.netty.channel.MessageEvent;

public interface ReceivedMessageCallBack {
	
	void received(Object msg, InetSocketAddress srcSocketAddress);

}
