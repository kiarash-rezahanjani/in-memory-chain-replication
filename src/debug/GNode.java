package debug;

public class GNode {
	public String id;
	public int stat;
	public Type type;
	public int capacity;

	public GNode(String id, int capacity, int stat, Type type){
		this.id=id;
		this.stat=stat;
		this.type=type;
		this.capacity=capacity;
	}

	enum Type{
		client, fserver,lserver
	}

}

