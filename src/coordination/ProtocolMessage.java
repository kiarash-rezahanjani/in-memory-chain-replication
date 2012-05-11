package coordination;

import java.io.Serializable;

public final class ProtocolMessage extends AbstractMessage{

	private static final long serialVersionUID = 1L;
	
	short msgType;
	Object msgContent;
	
	public ProtocolMessage(short msgType , Object msgContent ) {
		super(IDGenerator.getNextId());
		this.msgType = msgType;
		this.msgContent = msgContent;
	}
	
	public short getMessageType(){
		return msgType;
	}
	
	public void setMessageType(short msgType){
		this.msgType = msgType;
	}

	public Object getMsgContent() {
		return msgContent;
	}

	public void setMsgContent(Object msgContent) {
		this.msgContent = msgContent;
	}
}
