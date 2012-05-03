package persistence;

import java.util.List;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;
import ensemble.CircularBuffer;
import ensemble.Ensemble;


public abstract class AbstractPersister extends Thread{

	Ensemble ensemble;
	volatile boolean running = true; 

	public AbstractPersister(Ensemble ensemble){
		this.ensemble=ensemble;
	}

	/**
	 * Persist the entry and return the persisted notification.
	 * ATTENTION: This method should have an async equivaleny.
	 * @param entry
	 * @return true if entry is persisted
	 */
	public abstract boolean persistEntry(LogEntry entry);

	public LogEntry getPersistedMessage(LogEntry entry){
		return LogEntry.newBuilder().setMessageType(Type.ENTRY_PERSISTED)
				.setEntryId(entry.getEntryId()).build();
	}

	/**
	 * Remove entry from buffer and send persisted message to the predecessor.
	 * @param persistedMessage
	 * @throws Exception
	 */
	public void removePersistedEntry(final LogEntry persistedMessage) throws Exception{
		ensemble.getBuffer().remove(persistedMessage.getEntryId());
		ChannelFuture channelFuture = null;
		if(ensemble.getPredecessorChannel().isConnected())
			channelFuture = ensemble.getPredecessorChannel().write(persistedMessage);
		else
			throw new Exception("Predecessor channel is not connected!" +  ensemble.getPredecessorChannel());

		if(channelFuture!=null)
			channelFuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					// TODO Auto-generated method stub
					if(!future.isSuccess())
						throw new Exception("Persisted Message failed to deliver." +  future.getCause());
				}
			});
	}
	
	/**
	 * Remove entry from buffer and send persisted message to the predecessor.
	 * @param persistedMessage
	 * @throws Exception
	 */
	public void removePersistedEntryBcast(final LogEntry persistedMessage) throws Exception{
		ensemble.getBuffer().remove(persistedMessage.getEntryId());
		
		if(ensemble.getPredecessorChannel().isConnected())
			broadcastPersistedMessage( persistedMessage,ensemble.getpeersChannelHandle());
		else
			throw new Exception("Predecessor channel is not connected!" +  ensemble.getPredecessorChannel());


	}
	private void broadcastPersistedMessage(final LogEntry persistedMessage, final List<Channel> peersChannel) {
		for(Channel peer : peersChannel){
			peer.write(persistedMessage).addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					// TODO Auto-generated method stub
					if(future.isSuccess())
						;
				}
			});
		}
	}
	
	public void stopRunning(){
		running = false;
		interrupt();
	}

	public void run() {
		// TODO Auto-generated method stub
		int i = 0;
		while(running){
			try {
				LogEntry entry = ensemble.getBuffer().nextToPersist();
				boolean persisted = persistEntry(entry);
				Thread.sleep(2);//persist
				if(persisted)
					try {
						removePersistedEntryBcast(getPersistedMessage(entry));
						System.out.println("Message " + entry.getEntryId() + " Persisted by" + ensemble.getConfiguration().getBufferServerPort() + " Total No: "+  ++i);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

			}catch(InterruptedException e)
			{
				throw new RuntimeException("Persister was interrupted.");
			}

		}
	}
}
