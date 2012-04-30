package client;
import java.util.concurrent.atomic.AtomicLong;

public class IDGenerator {
	static AtomicLong id = new AtomicLong() ;
	static long getNextId(){
		long idNumber = id.getAndIncrement();
		if(idNumber >= Long.MAX_VALUE)
			id.set(0);
		return idNumber ;

	}
}
