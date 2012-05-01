package persistence;

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
	 * ATTENTION: This method should change to async method.
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
		if(ensemble.getPredecessorChannel().isOpen())
			channelFuture = ensemble.getPredecessorChannel().write(persistedMessage);
		else
			throw new Exception("Predecessor channel is Closed!" +  ensemble.getPredecessorChannel());

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
						removePersistedEntry(getPersistedMessage(entry));
						System.out.println("Persisted No:"+ ++i + entry.getEntryId());
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
