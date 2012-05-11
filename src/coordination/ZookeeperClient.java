package coordination;


import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.AsyncCallback;

import utility.Configuration;
import utility.NetworkUtil;
import coordination.Znode.EnsembleData;
import coordination.Znode.ServerData;
import coordination.Znode.ServersGlobalView;


import com.google.protobuf.InvalidProtocolBufferException;



public class ZookeeperClient implements Closeable{

	static ZooKeeper zk = null;
	String zkConnectionString ;
	int sessionTimeOut = 3000;
	String nameSpace ;
	String serverRootPath ;
	String serversGlobalViewPath;//logServiceRootPath + "/serversFullView";
	String ensembleRootPath ;
	String clientRootPath ;
	String myServerZnodePath;
	String ensembleMembersZnodeName = "ensembleMembers"; 
	Configuration config;

	//connect to zookeeper and register a wacher object
	public ZookeeperClient( Watcher watcher, Configuration config) throws KeeperException, IOException, InterruptedException
	{
		this.config = config;
		zkConnectionString = config.getZkConnectionString();
		sessionTimeOut = config.getZkSessionTimeOut();
		nameSpace = config.getZkNameSpace();
		serverRootPath = nameSpace + config.getZkServersRoot();
		serversGlobalViewPath = nameSpace + config.getZkServersGlobalViewRoot();//logServiceRootPath + "/serversFullView";
		ensembleRootPath = nameSpace + config.getZkEnsemblesRoot();

		if(zk==null)
		{
			zk = new ZooKeeper(zkConnectionString, sessionTimeOut, watcher);	
			createRoot(nameSpace);
			createRoot(serverRootPath);
			createRoot(serversGlobalViewPath);
			createRoot(ensembleRootPath);
		}
		
		myServerZnodePath = serverRootPath + "/" + getHostColonPort( config.getProtocolSocketAddress().toString() ); //( socketAddress != null ? socketAddress : new Random().nextInt(100000) );
	}
	
	public String getHostColonPort(String socketAddress)
	{
		String host = "";
		int port=0;

		Pattern p = Pattern.compile("^\\s*(.*?):(\\d+)\\s*$");
		Matcher m = p.matcher(socketAddress);

		if (m.matches()) 
		{
			host = m.group(1);
			
			if(host.contains("/"))
				host = host.substring( host.indexOf("/") + 1 );
			
			port = Integer.parseInt(m.group(2));

			return host + ":" + port;
		}else
			return null;

	}
	
	
	/**
	 * As the node might be created between time that exists method is returned and 
	 * the time that create method is invoked NodeExists exception is checked. 
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	void createRoot(String path) throws KeeperException, InterruptedException
	{
		try {
			Stat s = zk.exists(path, false);
			if(s==null)
				zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	



	public String getMyServerZnodePath() {
		return myServerZnodePath;
	}


	//-------------------------------------------------------------------------------------------------- Server Nodes ---------------------------------------------------------------
	//deprecated
	public boolean createServerZnode(ServerData data) throws KeeperException, InterruptedException
	{
		Stat s = zk.exists(myServerZnodePath, false);
		if(s==null)
		{
			zk.create(myServerZnodePath, data.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			return true;
		}
		return false;
	}
	
	//For testing Only
	public boolean createServerZnode(String nameNode, ServerData data) throws KeeperException, InterruptedException
	{
		String path = serverRootPath + "/" +  nameNode;
		Stat s = zk.exists(path, false);
		if(s==null)
		{
			zk.create(path, data.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			return true;
		}
		return false;
	}

	//if node exist update it otherwise create it with the given data
	public void updateServerZnode(ServerData data) throws KeeperException, InterruptedException
	{

		if(!createServerZnode(data))
			zk.setData(myServerZnodePath, data.toByteArray(), -1);	
	}

	public void deleteServerZnode() throws KeeperException, InterruptedException
	{
		Stat s = zk.exists(myServerZnodePath, false);
		if(s!=null)
			zk.delete(myServerZnodePath, -1);
	}

	public ServerData getServerZnodeDataByNodeName(String nodeName) throws KeeperException, InterruptedException, InvalidProtocolBufferException
	{
		Stat s = new Stat();
		byte[] data = zk.getData(serverRootPath + "/" + nodeName, false, s);
		return ServerData.parseFrom(data);
	}
	
	public ServerData getServerZnodeDataByProtocolSocketAddress(InetSocketAddress socketAddress) throws KeeperException, InterruptedException, InvalidProtocolBufferException
	{
		return getServerZnodeDataByNodeName( getHostColonPort(socketAddress.toString()) );
	}
	
	//testing
	public ServerData getServerZnodeDatabyFullPath(String path) throws KeeperException, InterruptedException, InvalidProtocolBufferException
	{
		Stat s = new Stat();
		byte[] data = zk.getData(path, false, s);
		return ServerData.parseFrom(data);
	}
	
	public List<String> getServerList() throws KeeperException, InterruptedException
	{
		return zk.getChildren(serverRootPath, false);
	}
	
	public Stat setServerFailureDetector(String nodeName)
	{
		Stat stat=null;
		try {
			stat = zk.exists(serverRootPath + "/" + nodeName, true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		} 
		return stat;
	}
	
	public Stat setServerFailureDetector(InetSocketAddress protocolSocketAddress)
	{
		return setServerFailureDetector(getHostColonPort(protocolSocketAddress.toString()));
	}

	//--------------------------- Ensemble-----------------------------------------------------------------------

	/**
	 * Create a permanent Znode as ensemble Znode.
	 * @param Initial data
	 * @return The actual path of the Znode
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String createEnsembleZnode(EnsembleData data) throws KeeperException, InterruptedException
	{
		String ensemblePath = zk.create(ensembleRootPath+"/ensemble-", data.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		//zk.create( ensemblePath + "/" + ensembleMembersZnodeName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
		return ensemblePath;
	}

	public void createMyEnsembleEmphemeralZnode(String ensemblePath, String myZnodeName) throws KeeperException, InterruptedException
	{
		zk.create( ensemblePath + "/" + ensembleMembersZnodeName + "/" + myZnodeName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	public void updateEnsembleZnode(String ensemblePath, EnsembleData data) throws KeeperException, InterruptedException
	{
		zk.setData(ensemblePath, data.toByteArray(), -1);
	}

	public void deleteEnsembleZnode(String ensemblePath) throws InterruptedException, KeeperException
	{
		zk.delete(ensemblePath, -1);
	}

	public EnsembleData getEnsembleData(String ensemblePath) throws KeeperException, InterruptedException, InvalidProtocolBufferException
	{
		EnsembleData data = EnsembleData.parseFrom(zk.getData(ensemblePath,false, null));
		return data;
	}
	//leader operation

	//failure operation


	//--------------------------------------------------------------------------------
	public boolean exists(String path) throws KeeperException, InterruptedException
	{
		Stat s = zk.exists(path, false);
		if(s==null)
			return false;
		else
			return true;
	}
	//choose subset of all children of server node and sort them, nodes are chosen randomly and there is a lower limit of number of retured candidates 
	//working fine
	public List<ServerData> getRandomEnsembleCandidates() throws KeeperException, InterruptedException, InvalidProtocolBufferException
	{
		float subSetFraction = 0.1f;
		int minCandidates = 5;
		int numCandidates = minCandidates;
		boolean returnAll = false;


		List<String> children = zk.getChildren(serverRootPath, false);

		if( minCandidates >= children.size() )
		{
			returnAll=true;
			numCandidates = children.size();
		}else
		{
			if( minCandidates >= subSetFraction*children.size() )
			{
				numCandidates = minCandidates;
			}else
			{
				numCandidates = Math.round((subSetFraction * children.size()) );
			}
		}

		List<ServerData> sortedCandidates = new LinkedList<ServerData>(); 
		//System.out.println("Number of children:" + children.size() + "\n");

		int nextCandidateIndex=0;
		int sortedCandidatesSize=0;
		Random rnd = new Random();
		for(int i = 0 ; i < numCandidates; i++)
		{

			if(returnAll==true)
				nextCandidateIndex=i;
			else
				nextCandidateIndex=rnd.nextInt(children.size());

			//if the element has been selected start a new iteration
			if( returnAll==false && sortedCandidates.contains(children.get(nextCandidateIndex)) )
			{
				i--;
				continue;
			}


			Stat stat = new Stat();
			byte[] data = zk.getData(serverRootPath + "/" + children.get(i), false, stat);

			ServerData dataObject = ServerData.parseFrom(data);

			sortedCandidatesSize = sortedCandidates.size();

			boolean addedInLoop = false;
			for(int j=0; j<sortedCandidatesSize ;j++)
			{
				if(dataObject.getCapacityLeft() >= sortedCandidates.get(j).getCapacityLeft())
				{	
					sortedCandidates.add(j, dataObject);
					addedInLoop = true;
					break;
				}
			}
			//add first element and the smallest elements to the tail
			if(addedInLoop==false)
				sortedCandidates.add(dataObject);

		}

		return sortedCandidates;
	}

	//---------------------------------------------------Servers View Snapshot----------------------
	
	//sort all the servers based on the capacity left using insertion sort, those with max capacity come in the beginning
	public List<ServerData> getSortedServersList() throws KeeperException, InterruptedException, InvalidProtocolBufferException
	{
		List<ServerData> sortedServers = new LinkedList<ServerData>(); 
		List<String> children = zk.getChildren(serverRootPath, false);
		
		for(int i = 0 ; i < children.size(); i++)
		{
			//Stat stat = new Stat();
			
			byte[] data = zk.getData(serverRootPath + "/" + children.get(i), false, null);

			ServerData serverData = ServerData.parseFrom(data);

	
			boolean addedInLoop = false;
			for(int j=0; j < sortedServers.size() ;j++)
			{
				if(serverData.getCapacityLeft() >= sortedServers.get(j).getCapacityLeft())
				{	
					sortedServers.add(j, serverData);
					addedInLoop = true;
					break;
				}
			}
			//add first element and the smallest elements to the tail
			if(addedInLoop==false)
				sortedServers.add(serverData);
		}

		return sortedServers;
	}
	
	void updateServersGlobalViewZnode(ServersGlobalView data) throws KeeperException, InterruptedException
	{

		Stat s = zk.exists(serversGlobalViewPath, false);
		if(s==null)
			zk.create(serversGlobalViewPath, data.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		else 
			zk.setData(serversGlobalViewPath, data.toByteArray(), -1);	
	}
	
	public ServersGlobalView getServersGlobalView() throws InvalidProtocolBufferException, KeeperException, InterruptedException
	{
		byte[] data = zk.getData(serversGlobalViewPath, null, null);
		
		if(data==null || data.length==0)
			return null;
		else
			return ServersGlobalView.parseFrom(data);
	}
/*	
	//update the sortedServers node with sortedServers and the index of the leaders, each leader is in charge of all nodes till next leader in the list
	public void updateServersGlobalViewZnode() throws InvalidProtocolBufferException, KeeperException, InterruptedException
	{
		List<ServerData> sortedServers = sortedServersList();
		applyEliminationPolicy(sortedServers);
		List<Integer> leaderIndexList = leaderIndexList(sortedServers);
		
		ServersGlobalView.Builder data = ServersGlobalView.newBuilder();
		data.addAllSortedServers(sortedServers);
		data.addAllLeaderIndex(leaderIndexList);
		
		updateServersGlobalViewZnode(data.build());
	}

	//implement the policy which server can participate i a chain
	void applyEliminationPolicy(List<ServerData> sortedServers)
	{

	}
	
	List<Integer> leaderIndexList(List<ServerData> sortedServers)
	{
		List<Integer> leaderIndexList = new ArrayList<Integer>();
		
		for(int i = 0; i < sortedServers.size(); i++)
			if(i%3==0) //&& sortedServers.size()-i>=3
				leaderIndexList.add(i);
			
		return leaderIndexList;
	}

	//if node exist update it otherwise create it with the given data

	//---------------------------------------------------------------------------------------------------
	

	public void printChildrenStat(String path) throws KeeperException, InterruptedException, InvalidProtocolBufferException
	{
		List<String> children = zk.getChildren(path, false);
		System.out.println("Number of children:" + children.size() + "\n");

		for(int i = 0 ; i < children.size(); i++)
		{

			System.out.println("\n------------CHILD------------\n" + children.get(i));

			Stat stat = new Stat();
			byte[] data = zk.getData(path+"/"+children.get(i), false, stat);

			System.out.println( "No of bytes: " + data.length );

			if(path == serverRootPath)
			{
				ServerData dataObject = ServerData.parseFrom(data);
				System.out.print( "STAT: " + stat.toString() );
				System.out.print( "DATA: " + dataObject.toString() );
			}

			if(path == ensembleRootPath)
			{
				EnsembleData dataObject = EnsembleData.parseFrom(data);
				System.out.print( "STAT: " + stat.toString() );
				System.out.print( "DATA: " + dataObject.toString() );
			}
		}

	}



	
	public static void main(String[] args)
	{
		try {
			ZookeeperClient zkCli = new ZookeeperClient(null);

			List<ServerData> dataList = new ArrayList<ServerData>();

			for(int i=0;i<101;i++)
			{
				int rndCapacity = new Random().nextInt(90) + 10;
				int rndnameNode = new Random().nextInt(50000);
				
				ServerData.Builder data1 = ServerData.newBuilder();
				data1.setCapacityLeft(rndCapacity);
				data1.setSocketAddress("localhost"+ rndnameNode);
				data1.setStat(ServerData.Status.ACCEPT_ENSEMBLE_REQUEST);

				boolean created = zkCli.createServerZnode(  String.valueOf(rndnameNode), data1.build());
				System.out.println(created);
				Thread.sleep(100);
			}

			

//			zkCli.printChildrenStat(zkCli.serverRootPath);
//			zkCli.updateServersGlobalViewZnode();
			ServersGlobalView sortedServers = zkCli.getServersGlobalView();

			System.out.println("\n-----------------SORTED-----------------------\n");

			List<ServerData> serverData = sortedServers.getSortedServersList();
			List<Integer> leader = sortedServers.getLeaderIndexList();
			System.out.println("No of Servers Sorted: "+ serverData.size());
			for(ServerData sd: serverData)
			{
				System.out.print(sd.getCapacityLeft() + " ");
			}
			System.out.print("\n\n");
			System.out.print(leader);

			//System.out.print(zkOpt.getLocalInetSocket());

		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
*/

	@Override
	public void close() throws IOException 
	{
		try {
			zk.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	
}

