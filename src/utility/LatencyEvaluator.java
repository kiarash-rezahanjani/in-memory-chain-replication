package utility;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import client.Log.LogEntry.Identifier;;

public class LatencyEvaluator{
	HashMap<Identifier,ElapsedTime> elapsedTime = new HashMap<Identifier,ElapsedTime>(); 
	TextFile file = new TextFile("evaluation_results");
	long start , end;

	public LatencyEvaluator(){
		
	}

	public void sent(Identifier id){
		if(elapsedTime.size()==0)
			start = System.nanoTime();
		elapsedTime.put(id, new ElapsedTime());
	}

	public void received(Identifier id){
		elapsedTime.get(id).setEndTime();
	}

	public void report(){
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
		String report = "Average Latency " + average/success + " success/Total " + success + "/" + elapsedTime.size() + " Throughput " +  (success/(double)(end-start))*1000000000;
	//	System.out.println(report);
		file.print(report);
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
		public void setEndTime(){
			end = System.nanoTime();
			acked = true;
		}
		
		public boolean isSucces(){
			return acked;
			
		}

		public long getElapsedTime(){
			if(acked)
				return end-start;
			else
				return 0;
		}

	}

}
