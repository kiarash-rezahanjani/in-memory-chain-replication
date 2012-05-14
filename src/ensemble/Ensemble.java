package ensemble;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import persistence.AbstractPersister;
import persistence.DummyPersister;

import client.Log.LogEntry;
import client.Log.LogEntry.Identifier;
import client.Log.LogEntry.Type;

import utility.Configuration;

public class Ensemble {
	final Buffer buffer;
	List<InetSocketAddress> sortedChainSocketAddress;
	Channel successorChannel;//to send logs
	Channel predecessorChannel;//to send remove message
	List<Channel> peersChannel = new ArrayList<Channel>();//contains channels connecting to all members of the ensemble including itself, use to broadcast the messages
	HashMap<String, Channel> headDbClients = new HashMap<String, Channel>();//receive logs <clientId, channel>
	HashMap<String, Channel> tailDbClients = new HashMap<String, Channel>();//send ack
	HashMap<String, Long> lastDeliveredMessage = new HashMap<String,Long>();//<clientId, msgId> last message acknowledged to client to delivered to next buffer server
	HashMap<String, Long> lastPersistedMessage = new HashMap<String,Long>();//<clientId, msgId> last message persisted to client to delivered to next buffer server
	HashMap<String, Identifier> clientFailed = new HashMap<String,Identifier>();
	
	final Configuration conf;

	AbstractPersister persister;
	BufferReader bufferReader;

	public Configuration getConfiguration() {
		return conf;
	}
	public Channel getSuccessorChannel() {
		return successorChannel;
	}
	public Channel getPredecessorChannel() {
		return predecessorChannel;
	}
	public HashMap<String, Channel> getHeadDbClients() {
		return headDbClients;
	}
	public HashMap<String, Channel> getTailDbClients() {
		return tailDbClients;
	}

	public Ensemble(Configuration conf, List<InetSocketAddress> sortedChainSocketAddress) throws Exception{
		this.conf=conf;
		this.sortedChainSocketAddress = sortedChainSocketAddress;
		if(sortedChainSocketAddress.size()<2)
			throw new Exception("obj.chain Ensemble size < 2 ");
		//	buffer = new NaiveCircularBuffer(conf.getEnsembleBufferSize(),tailDbClients.keySet());
		//buffer = new ConcurrentBuffer(conf.getEnsembleBufferSize(),tailDbClients.keySet());
		buffer = new HashedBuffer(conf.getEnsembleBufferSize(),tailDbClients.keySet());
		persister = new DummyPersister(this);
		bufferReader = new BufferReader(this);
		persister.start();
		bufferReader.start();

		print("persister"+persister.getName());
		print("reader"+bufferReader.getName());
	}

	//for testing : this hould be done through zookeeper
	void channelClosed(Channel channel){
		if(tailDbClients.values().contains(channel)){
			Iterator it = tailDbClients.entrySet().iterator();
			String clientId = null;
			while(it.hasNext()){
				Map.Entry<String, Channel> entry =(Map.Entry<String, Channel>) it.next();
				if(channel.equals(entry.getValue())){
					clientId = entry.getKey();
					Identifier id = Identifier.newBuilder().setClientId(clientId).setMessageId(lastDeliveredMessage.get(clientId).longValue()).build();
					broadcastChannel(LogEntry.newBuilder().setMessageType(Type.LAST_ACK_SENT_TO_FAILED_CLIENT)
							.setEntryId(id).build());
					entry.getValue().close();
					tailDbClients.remove(clientId);
					return;
				}				
			}	
		}
		
		if(headDbClients.values().contains(channel)){
			Iterator it = headDbClients.entrySet().iterator();
			String clientId = null;
			while(it.hasNext()){
				Map.Entry<String, Channel> entry =(Map.Entry<String, Channel>) it.next();
				if(channel.equals(entry.getValue())){
					entry.getValue().close();
					headDbClients.remove(clientId);
					return;
				}				
			}	
		}
	}

	public void clientFailed(Identifier lastAck){
		//remove the head or tail
		//send the last ack to the servers
		//garbage collect
		//persist
		//remove client from all data structures
/*		clientFailed.put(lastAck.getClientId(), lastAck);
		
		if(tailDbClients.containsKey(lastAck.getClientId()))
			tailDbClients.get(lastAck.getClientId()).close();
	
		if(headDbClients.containsKey(lastAck.getClientId()))
			headDbClients.get(lastAck.getClientId()).close();
*/		
		print("Last Acked ID: " + lastAck);
		buffer.garbageCollect(lastAck);
	}

	public void releaseResourcesOfFailedClient(String clientId){

	}

	public List<Channel>  getPeersChannelHandle(){
		return peersChannel;
	}

	public HashMap<String, Long> getLastDeliveredMessageHandle(){
		return lastDeliveredMessage;
	}

	public HashMap<String, Long> getLastPersistedMessageHandle(){
		return lastPersistedMessage;
	}

	public InetSocketAddress getSuccessorSocketAddress() throws Exception{
		int index = getLocalAddressIndex();
		if(index<0)
			throw new Exception("Obj.Ensemble. local index = -1");
		index= (index+1)%sortedChainSocketAddress.size();
		return sortedChainSocketAddress.get(index);
	}
	public InetSocketAddress getPredessessorSocketAddress() throws Exception{
		int index = getLocalAddressIndex();
		if(index<0)
			throw new Exception("Obj.Chain. local index = -1");
		index= (index+sortedChainSocketAddress.size()-1)%sortedChainSocketAddress.size();
		return sortedChainSocketAddress.get(index);
	}
	int getLocalAddressIndex(){
		for(int i = 0; i<sortedChainSocketAddress.size(); i++)
			if(sortedChainSocketAddress.get(i).equals(conf.getBufferServerSocketAddress()))
				return i;
		return -1;				
	}
	public List<InetSocketAddress> getSortedChainSocketAddress() {
		return sortedChainSocketAddress;
	}
	public void setSortedChainSocketAddress(
			List<InetSocketAddress> sortedChainSocketAddress) {
		this.sortedChainSocketAddress = sortedChainSocketAddress;
	}
	public void setSuccessor(Channel successor) {
		this.successorChannel = successor;
	}
	public void setPredecessor(Channel predecessor) {
		this.predecessorChannel = predecessor;
	}
	public Buffer getBuffer() {
		return buffer;
	}

	public void broadcastChannel(final LogEntry message) {
		for(Channel peer : peersChannel){
			peer.write(message).addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					// TODO Auto-generated method stub
					if(future.isSuccess())
						;
				}
			});
		}
	}

	public boolean addHeadDBClient(String clientId, String clientSocketAddress, Channel channel) throws Exception{
		LogEntry tailNotify = LogEntry.newBuilder()
				.setMessageType(Type.TAIL_NOTIFICATION)
				.setEntryId(Identifier.newBuilder().setClientId(clientId))
				.setClientSocketAddress(clientSocketAddress)
				.build();
		ChannelFuture future;
		if(predecessorChannel.isConnected())
			future = predecessorChannel.write(tailNotify);
		else
			throw new Exception("Predecessor channel is Not Connected!" + predecessorChannel);
		//	if(future.isSuccess()){
		headDbClients.put(clientId, channel);
		return true;
		//}
		//	else
		//		return false;
	}
	public void removeHeadDbClient(String clientId){
		headDbClients.remove(clientId);
	}
	public void addTailDBClient(String clientId, Channel tailChannel){
		tailDbClients.put(clientId, tailChannel);
	}
	public void removeTailDbClient(String clientId){
		tailDbClients.remove(clientId);
	}
	public void close(){
		successorChannel.close();
		predecessorChannel.close();
		for(Channel ch : tailDbClients.values())
			ch.close();
		persister.stopRunning();
		bufferReader.stopRunning();
	}

	public void entryPersisted(final LogEntry entry) throws Exception{
		buffer.remove(entry.getEntryId());
		/*		
		ChannelFuture channelFuture = null;
		if(!headDbClients.containsKey(entry.getEntryId().getClientId()))
			if(predecessorChannel.isConnected())
				channelFuture = predecessorChannel.write(entry);
			else{

				throw new Exception("Predecessor channel is Not Connected!" +  predecessorChannel
						+ predecessorChannel.isBound() +predecessorChannel.isConnected() + 
						predecessorChannel.isOpen() + predecessorChannel.isWritable());
			}
		if(channelFuture!=null)
			channelFuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					// TODO Auto-generated method stub
					if(!future.isSuccess())
						throw new Exception("Persisted Message failed to deliver." + Thread.currentThread() + " Channel: "+ future.getChannel() + " " +  future.getCause());
				}
			});
		 */
	//	print(" Message " + entry.getEntryId() + " Removed by " + conf.getBufferServerPort() );
	}

	public void addToBuffer(final LogEntry entry) throws Exception{
		/*		Channel channel = tailDbClients.get(entry.getEntryId().getClientId());
		ChannelFuture future;
		if(channel==null)
			channel = successorChannel;

		if(channel==null || !channel.isOpen())
			throw new Exception("Attempt to send data while channel is not open or its null. Chenl: " + channel );
		else
			future = channel.write(ackMessage(entry.getEntryId()));

		future.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				// TODO Auto-generated method stub
				if(!future.isSuccess())
					throw new Exception("Failed to send entry to destination(Ack t client or the log to next buffer server)." + future.getCause());
				else
					System.out.println("Msg Buffered and delivered" +  entry.getEntryId());
			}
		});
		 */	buffer.add(entry);
		 //print("Buffered " + entry.getEntryId().getMessageId() );

	}
	void print(String str){
		//String meta = "\n[RP: "+readPosition + " WP: " + writePosition + " PS: " + 
		//		persistIndexQueue.size() + " c/s: " + size.get() + "/"+ capacity + " \n"; 
		System.out.println("ENSEMBLE "+ str );
		}

	public void removeClient(){
		
	}
	public void shutdownEnsemble(){
		
	}
}
