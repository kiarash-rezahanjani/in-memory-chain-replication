package coordination.rpc;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;

import coordination.ReceivedMessageCallBack;

//import com.google.common.util.concurrent.ThreadFactoryBuilder;
import utility.Configuration;

public final class UdpServer {		
        public static final int DEFAULT_PORT = 1111;          
        private ChannelFactory factory;        
        private ConnectionlessBootstrap bootstrap;       
        private volatile boolean running = false;
        private ExecutorService executorService;
        private Channel serverChannel;
        private ReceivedMessageCallBack callback;        
        Configuration config ;
     
        public UdpServer(Configuration config , ReceivedMessageCallBack callback) {
                this.config = config;
                this.callback = callback;
        }
        
        void start() throws IOException {                
                executorService = Executors.newCachedThreadPool();                
                factory = new NioDatagramChannelFactory(executorService);
                bootstrap = new ConnectionlessBootstrap(factory);               
                bootstrap.setPipelineFactory(new UdpServerPipelineFactory(callback));                
                bootstrap.setOption("reuseAddress", true);
                bootstrap.setOption("tcpNoDelay", true);
                bootstrap.setOption("broadcast", false);
                bootstrap.setOption("sendBufferSize", 1024);
                bootstrap.setOption("receiveBufferSize", 1024);              
                serverChannel = bootstrap.bind(config.getProtocolSocketAddress());
                running = true;
        }
        
        public void stop() {
                System.out.println("stopping UDP server..");                
                serverChannel.close();
//              factory.releaseExternalResources();
                bootstrap.releaseExternalResources();               
                running = false;                             
                System.exit(0);
        }

        public boolean isRunning() {
                return running;
        }
}
