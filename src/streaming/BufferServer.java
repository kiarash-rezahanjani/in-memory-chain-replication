package streaming;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
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
import ensemble.ChainManager;
import ensemble.ClientServerCallback;
import ensemble.Ensemble;

import utility.Configuration;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class BufferServer {
	ServerBootstrap bootstrap;
	NioServerSocketChannelFactory factory;
	ChannelPipelineFactory channelPipeline;
	Configuration conf;

	public BufferServer(Configuration conf, final ClientServerCallback callback){	
		this.conf = conf;
		factory=new NioServerSocketChannelFactory(
				Executors.newSingleThreadExecutor(),
				Executors.newCachedThreadPool());

		channelPipeline = new ChannelPipelineFactory(){

			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline p = pipeline();
				p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
				p.addLast("protobufDecoder", new ProtobufDecoder(Log.LogEntry.getDefaultInstance()));
				p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
				p.addLast("protobufEncoder", new ProtobufEncoder());
				p.addLast("handler", new BufferServerHandler(callback));
				/*		               
				p.addLast("framer", new DelimiterBasedFrameDecoder(
						8192, Delimiters.lineDelimiter() ));
				 p.addLast("decoder", new StringDecoder());
				 p.addLast("encoder", new StringEncoder());
				 p.addLast("handler", new BufferServerHandler(buffer));  */
				return p;
			}
		};

		ServerBootstrap bootstrap = new ServerBootstrap(factory);
		bootstrap.setPipelineFactory(channelPipeline);
		bootstrap.setOption("tcoNoDelay", true);
		Channel ch = bootstrap.bind(conf.getBufferServerSocketAddress());
		
		System.out.println(" Is running." + ch.getLocalAddress());
	}

	public void stop(){
		if(factory!=null)
			factory.releaseExternalResources();
		if(bootstrap!=null)
			bootstrap.releaseExternalResources();
	}
}
