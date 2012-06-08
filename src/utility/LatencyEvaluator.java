package utility;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import client.Log.LogEntry.Identifier;;

public class LatencyEvaluator{
	HashMap<Identifier,ElapsedTime> elapsedTime = new HashMap<Identifier,ElapsedTime>(); 
	TextFile file ;
	long start , end;
	long firstTime , lastTime, noRequest=0;
	long average = 0;
	long noAck = 0 ;
	long startTime ;
	long endTime;

	public LatencyEvaluator(String outputFile){
		file = new TextFile(outputFile);
	}

	public void sent(Identifier id){
		if(noRequest==5000)
			firstTime =System.nanoTime();
		
		if(noAck==0)
			startTime= System.nanoTime();

		if(elapsedTime.size()==0)
			start = System.nanoTime();
		elapsedTime.put(id, new ElapsedTime());
	}

	public void received(Identifier id){
		average += elapsedTime.get(id).setEndTime().getElapsedTime();
		noAck ++;
		noRequest++;
		
		if(noRequest%100000==99999){
			lastTime =System.nanoTime();
			String report = " Throughput " +  ((noRequest-5000)/(double)(lastTime-firstTime))*1000000000 + "\n";
			System.out.println("\n\n THROUGHPUT "+ report + "\n\n");
		}
	}

	public void report(){
		endTime= System.nanoTime();

		String report = "Average Latency(ns) " + average/noAck +  " Throughput " +  (noAck/(double)(endTime-startTime))*1000000000 + "\n";
		average=0;
		noAck=0;
		System.out.println(report);
	}

	public void reportDeprecated(){
		end = System.nanoTime();
		Iterator it =  elapsedTime.entrySet().iterator();
		long average = 0;
		int success = 0;
		while(it.hasNext()){
			Map.Entry<Identifier, ElapsedTime> entry = (Map.Entry<Identifier, ElapsedTime>) it.next();
			ElapsedTime time = entry.getValue();
			if(time.isSucces()){
				success++;
				average += time.getElapsedTime();
			}
		}
		String report = "Average Latency ns " + average/success + " success/Total " + success + "/" + elapsedTime.size() + " Throughput " +  (success/(double)(end-start))*1000000000 + "\n";
		System.out.println(report);
		file.print(report);
		elapsedTime.clear();
	} 

	class ElapsedTime{
		long start;
		long end;
		boolean acked = false;

		public ElapsedTime(){
			setStartTime();
		}

		public void setStartTime(){
			start = System.nanoTime();
		}
		public ElapsedTime setEndTime(){
			end = System.nanoTime();
			acked = true;
			return this;
		}

		public boolean isSucces(){
			return acked;

		}

		/**
		 * get latency for the message, received time - sent time
		 * @return received time - sent time
		 */
		public long getElapsedTime(){
			if(acked)
				return end-start;
			else
				return 0;
		}

	}

	public void clean(){
		elapsedTime.clear();
		file.close();
	}
}
