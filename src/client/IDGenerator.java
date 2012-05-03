package client;
import java.util.concurrent.atomic.AtomicLong;

public class IDGenerator {
	AtomicLong id = new AtomicLong() ;
	public long getNextId(){
		long idNumber = id.getAndIncrement();
		if(idNumber >= Long.MAX_VALUE)
			id.set(0);
		return idNumber ;

	}
}
