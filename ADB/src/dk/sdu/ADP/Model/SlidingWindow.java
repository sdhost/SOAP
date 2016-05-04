package dk.sdu.ADP.Model;

import java.util.concurrent.LinkedBlockingQueue;

// SlidingWindow is a special queue with a size limit noted as windowSize and 
// auto delete the oldest record if the queue size reach the windowSize

public class SlidingWindow extends LinkedBlockingQueue<String> {
	
	private int windowSize;
	
	public SlidingWindow(int size){
		super();
		this.windowSize = size;
	}
	
	public boolean isFull(){
		return this.size() >= windowSize;
	}
	
	public String insert(String newRecord) throws InterruptedException{
		if(this.size() < this.windowSize){
			this.put(newRecord);
			return null;
		}else{
			String oldkey = this.take();
			this.put(newRecord);
			return oldkey;
		}
	}
	
	
	
}
