package streaming;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import ensemble.ClientServerCallback;
import ensemble.Ensemble;

public class BufferClientHandler extends SimpleChannelHandler{

	ClientServerCallback callback;
	public BufferClientHandler(ClientServerCallback callback){
		this.callback = callback;
	}
	
    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
    	//callback.clientReceivedMessage(e);
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
    	callback.exceptionCaught(e);
    }
    
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		callback.channelClosed(e);
		
	}
}
