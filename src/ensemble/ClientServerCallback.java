package ensemble;

import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;

public interface ClientServerCallback {

	public void serverReceivedMessage(MessageEvent e);
	public void clientReceivedMessage(MessageEvent e);
	public void channelClosed(ChannelStateEvent e);
	public void serverAcceptedConnection(ChannelStateEvent e);
	public void exceptionCaught(ExceptionEvent e);
}
