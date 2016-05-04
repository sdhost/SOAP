import java.util.concurrent.PriorityBlockingQueue;


public class TaskQueue extends PriorityBlockingQueue<Task>{
	
	int last_size = 0;
	
	public TaskQueue(){
		
		super();
		
	}
	
	public int getCount(){
		int size = this.size();
		size -= last_size;
		this.last_size = this.size();
		return size;
	}
}
