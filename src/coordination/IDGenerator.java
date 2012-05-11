package coordination;

import java.util.concurrent.atomic.AtomicLong;

public final class IDGenerator {
	
	static AtomicLong id = new AtomicLong() ;
	
	static long getNextId()
	{
		long idNumber = id.getAndIncrement();
		
		if(idNumber >= Long.MAX_VALUE)
			id.set(0);
			
		return idNumber ;
	}

}
