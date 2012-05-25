package ensemble;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

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
		future.addListener(new MessageFutureListener(entry.getEntryId())) ;
	}

	LogEntry ackMessage(Identifier id){
		return LogEntry.newBuilder().setEntryId(id)
				.setClientSocketAddress(ensemble.getConfiguration().getBufferServerSocketAddress().toString())
				.setMessageType(Type.ACK).build();
	}
	
	public void stopRunning(){
		running = false;
		//interrupt();
	}

	public class MessageFutureListener implements ChannelFutureListener{
		Identifier id;
		//int bufferIndex;
		public MessageFutureListener(Identifier id){
			this.id = id;
		//	ensemble.buffer.readComplete(id);
		}
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			// TODO Auto-generated method stub
			if(future.isSuccess()){
				ensemble.buffer.readComplete(id);
				ensemble.getLastDeliveredMessageHandle().put(id.getClientId(), id.getMessageId());
			}
			else{
			//	failedDelivery.add(id);
				//lastMessageDelivered.put(id.getClientId(), Long.valueOf(id.getMessageId()) ); 
			}
		}
		
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
				System.out.println("BufferReader.run(): Last delivered" + ensemble.lastDeliveredMessage);
				System.out.println("BufferReader.run(): Last persisted" + ensemble.lastPersistedMessage);
				System.exit(-1);
			}
		}

	}

}
