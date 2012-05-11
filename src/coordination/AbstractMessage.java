package coordination;

import java.io.Serializable;
import java.net.InetSocketAddress;

public abstract class AbstractMessage  implements Serializable{
	
	long msgId;
	InetSocketAddress socketAddress;
	public AbstractMessage(long msgId)
	{
		this.msgId = msgId;
	}

	public long getMsgId() {
		return msgId;
	}

	public void setMsgId(long msgId) {
		this.msgId = msgId;
	}

	public void setSrcSocketAddress(InetSocketAddress socketAddress)
	{
		this.socketAddress = socketAddress;
	}
	
	public InetSocketAddress getSrcSocketAddress()
	{
		return socketAddress;
	}

}
