import java.util.LinkedList;


public class Task implements Comparable<Task>{
	
	public String id;
	public Integer priority;
	public Long timestamp;
	public Long finishTime;
	public Integer frequency;
	public Integer no;
	public boolean isSingle = false;
	public boolean isPiggyback = false;
	public boolean piggybacked = false;
	public float write_rate = 0;
	public String piggyback_id = null;
	
	public LinkedList<String> query;
	
	public Task(Integer no, String id, LinkedList<String> query, Integer priority, Long timestamp, Integer frequency){
		this.no = no;
		this.id = id;
		this.priority = priority;
		this.timestamp = timestamp;
		this.frequency = frequency;
		this.query = query;
	}
	
	public void setPiggy(String id){
		this.piggyback_id = id;
	}
	
	public String getPiggy(){
		return this.piggyback_id;
	}

	@Override
	public int compareTo(Task t) {
		 int p1 = this.priority;  
         int p2 = t.priority;  
         long i1 = this.timestamp;  
         long i2 = t.timestamp;  
           
         //first compare by priority  
         //then compare by index  
         return p2 > p1 ? 1 : (p2 < p1 ? -1 :  
                 (i1 > i2 ? 1 : (i1 < i2 ? -1 : 0))); 
	}
}
