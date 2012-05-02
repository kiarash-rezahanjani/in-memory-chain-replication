package ensemble;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;

/**
 * Read the elements from buffer in order that they are written, 
 * if the server is the tail send an ack to client otherwise send
 * it to the next buffer server.
 * @author root
 *
 */
public class BufferReader  extends Thread{

	Ensemble ensemble;
	boolean running = true;
	//boolean ringComplete = false;
	public BufferReader(Ensemble ensemble){
		this.ensemble = ensemble;
	}

	public void sendToSuccessor(final LogEntry entry) throws Exception{
		ChannelFuture future;
		Channel channel = ensemble.getTailDbClients().get(entry.getEntryId().getClientId());
		if(channel!=null){//if I am the tail send ack 
			if(channel.isConnected())
				future = channel.write(ackMessage(entry.getEntryId()));
			else
				throw new Exception("Tail=>DBClient channel is not connected. Channel:" + channel);
		}else{//otherwise send to next the log buffer server
			channel = ensemble.getSuccessorChannel();
			if(channel==null)
				throw new Exception("Successor channel is null.");
			if(channel.isConnected())
				future = channel.write(entry);
			else
				throw new Exception("BufferServer=>BufferServer channel is not connected. Channel:" + channel);
		}

		future.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				// TODO Auto-generated method stub
				if(!future.isSuccess())
					throw new Exception("Failed to send entry to destination(Ack => client or the log => next buffer server)." + future.getCause());
				else
					System.out.println("Message " +  entry.getEntryId() + " delivered by " 
				+ ensemble.getConfiguration().getBufferServerPort() + " to " + future.getChannel().getRemoteAddress());
			}
		});
	}

	LogEntry ackMessage(Identifier id){
		return LogEntry.newBuilder().setEntryId(id)
				.setClientSocketAddress(ensemble.getConfiguration().getBufferServerSocketAddress().toString())
				.setMessageType(Type.ACK).build();
	}

	public void stopRunning(){
		running = false;
		interrupt();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(running){
			try {
				sendToSuccessor(ensemble.getBuffer().nextToRead());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
