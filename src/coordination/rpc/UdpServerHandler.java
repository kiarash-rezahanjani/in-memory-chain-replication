package coordination.rpc;

import java.net.InetSocketAddress;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import coordination.ProtocolMessage;
import coordination.ReceivedMessageCallBack;

public class UdpServerHandler extends SimpleChannelHandler {

	ReceivedMessageCallBack callback;
	
	public UdpServerHandler(ReceivedMessageCallBack callback)
	{
		this.callback = callback;
	}
	
	@Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            e.getCause().printStackTrace();
            e.getChannel().close();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
          //  System.out.println("SERVER: Message from:" + e.getRemoteAddress() + " Content:" + e.getMessage().toString());
    	callback.received(e.getMessage(), (InetSocketAddress) e.getRemoteAddress());
    
    }

}
