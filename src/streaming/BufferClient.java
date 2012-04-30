package streaming;

import static org.jboss.netty.channel.Channels.pipeline;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

import client.Log;
import client.Log.LogEntry;
import client.Log.LogEntry.Type;
import ensemble.ClientServerCallback;
import ensemble.Ensemble;

import utility.Configuration;

public class BufferClient {
	ClientBootstrap bootstrap;
	NioClientSocketChannelFactory factory;
	ChannelPipelineFactory channelPipeline;
	Configuration conf ;

	public BufferClient(Configuration conf, final ClientServerCallback callback) {
		this.conf = conf;
		factory = new NioClientSocketChannelFactory(
				Executors.newSingleThreadExecutor(),
				Executors.newCachedThreadPool());

		bootstrap = new ClientBootstrap(factory);

		channelPipeline = new ChannelPipelineFactory(){

			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = pipeline();
				p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
				p.addLast("protobufDecoder", new ProtobufDecoder(Log.LogEntry.getDefaultInstance()));
				p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
				p.addLast("protobufEncoder", new ProtobufEncoder());
				p.addLast("handler", new BufferClientHandler(callback));
				/*	        
				p.addLast("framer", new DelimiterBasedFrameDecoder(
						8192, Delimiters.lineDelimiter() ));
				 p.addLast("decoder", new StringDecoder());
				 p.addLast("encoder", new StringEncoder());
				 p.addLast("handler", new BufferServerHandler(buffer)); */	
				return p;
			}
		};
		bootstrap.setPipelineFactory(channelPipeline);
	}

	/**
	 * Asynchronously connect to the specified server and add a listener for connection attempt and channel identification message.
	 * If the channel attempt or identification notification fails it closes the channel. 
	 * @param Remote Server SocketAddress
	 * @return channel
	 */
	public Channel connectServerToServerAsync(InetSocketAddress remoteSocketAddress){

		ChannelFuture connectionChannelFuture = bootstrap.connect(remoteSocketAddress);
		Channel channel = connectionChannelFuture.getChannel();

		//listener for connection and notification message
		connectionChannelFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				// TODO Auto-generated method stub
				if(future.isSuccess()){
					ChannelFuture msgFuture =future.getChannel().write(LogEntry.newBuilder()
							.setMessageType(Type.CONNECTION_BUFFER_SERVER)
							.setClientSocketAddress(conf.getBufferServerSocketAddress().toString())
							.build());
					msgFuture.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							// TODO Auto-generated method stub
							if(!future.isSuccess())
								future.getChannel().close();
						}
					});
				}
				else
					future.getChannel().close();
			}
		});

		return channel;
	}

	/**
	 * Starts a connection to a remote server and returns the correspondent channel.
	 * If the connection attempts fails it returns null.
	 * @param remoteSocketAddress
	 * @return
	 */
	public Channel connectServerToServer(InetSocketAddress remoteSocketAddress){

		ChannelFuture channelFuture = bootstrap.connect(remoteSocketAddress);
		Channel channel = channelFuture.getChannel();
		if(!channelFuture.awaitUninterruptibly().isSuccess()){
			channelFuture.getCause().printStackTrace();
			return null;}
		else {
			ChannelFuture msgSentFuture = channel.write(LogEntry.newBuilder()
					.setMessageType(Type.CONNECTION_BUFFER_SERVER)
					.setClientSocketAddress(conf.getBufferServerSocketAddress().toString())
					.build());
			if(!msgSentFuture.awaitUninterruptibly().isSuccess())
				return null;
		}
		return channel;
	}

	/**
	 * Asynchronously connect to the specified server and add a listener for connection attempt and channel identification message.
	 * If the channel attempt or identification notification fails it closes the channel. 
	 * @param Remote Server SocketAddress
	 * @return channel
	 */
	public Channel connectServerToClient(InetSocketAddress remoteSocketAddress){

		ChannelFuture connectionChannelFuture = bootstrap.connect(remoteSocketAddress);
		Channel channel = connectionChannelFuture.getChannel();

		//listener for connection and notification message
		connectionChannelFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				// TODO Auto-generated method stub
				if(future.isSuccess()){
					ChannelFuture msgFuture =future.getChannel().write(LogEntry.newBuilder()
							.setMessageType(Type.CONNECTION_TAIL_TO_DB_CLIENT)
							.setClientSocketAddress(conf.getBufferServerSocketAddress().toString())
							.build());
					msgFuture.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							// TODO Auto-generated method stub
							if(!future.isSuccess())
								future.getChannel().close();
						}
					});
				}
				else
					future.getChannel().close();
			}
		});

		return channel;
	}

	/**
	 * Asynchronously connect to the specified server and add a listener for connection attempt and channel identification message.
	 * If the channel attempt or identification notification fails it closes the channel. 
	 * @param Remote Server SocketAddress
	 * @return channel
	 */
	public Channel connectClientToServer(InetSocketAddress remoteSocketAddress){
		System.out.println("\nCLIENT coneccting to " + remoteSocketAddress + "....\n");
		ChannelFuture connectionChannelFuture = bootstrap.connect(remoteSocketAddress);
		Channel channel = connectionChannelFuture.getChannel();

		//listener for connection and notification message
		connectionChannelFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				// TODO Auto-generated method stub
				if(future.isSuccess()){
					ChannelFuture msgFuture =future.getChannel().write(LogEntry.newBuilder()
							.setMessageType(Type.CONNECTION_DB_CLIENT)
							.setEntryId(LogEntry.Identifier.newBuilder().setClientId(conf.getDbClientId()))
							.setClientSocketAddress(conf.getBufferServerSocketAddress().toString())
							.build());
					msgFuture.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							// TODO Auto-generated method stub
							if(!future.isSuccess())
								future.getChannel().close();
						}
					});
				}
				else
					future.getChannel().close();
			}
		});
		return channel;
	}

	public void stop(){
		if(factory!=null)
			factory.releaseExternalResources();
		if(bootstrap!=null)
			bootstrap.releaseExternalResources();
	}

}
