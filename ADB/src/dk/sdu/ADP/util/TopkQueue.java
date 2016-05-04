package dk.sdu.ADP.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

// The priorty will delete the head element if facing a put operation and the
// current size is equal to the preset limit
// Only use the put(E e) for data insert


public class TopkQueue<E> extends PriorityBlockingQueue<E>{

	private int size;
	
	public TopkQueue(int ksize){
		super();
		this.size = ksize;
	}
	
	public void put(E e){
		if(this.size() < this.size)
			super.put(e);
		else{
			super.poll();
			super.put(e);
		}
	}
	
	public int getFreq(String key){
		int freq = 0;
		for(Iterator it = this.iterator();it.hasNext();){
			Object ins = it.next();
			if(ins instanceof Pair){
				Pair p = (Pair) ins;
				if(key.equalsIgnoreCase(p.getKey()))
					return p.getValue();
			}else{
				System.out.println("Not Supported gettopk!  " + ins.getClass());
				return -1;//Return an empty result set
			}
		}
		return -1;
	}
	
	public List<String> gettopk(int k, boolean descending){
		List<String> tmp = new LinkedList<String>();
		List<String> result = new LinkedList<String>();
		int realk;
		if(k >= this.size()){
			realk = this.size();
		}else
			realk = k;
		
		int count = 1;
		for(Iterator it = this.iterator();it.hasNext();){
			
			String txn;
			Object ins = it.next();
			if(ins instanceof Pair){
				Pair p = (Pair) ins;
				txn = p.getKey();
			}else{
				System.out.println("Not Supported gettopk!  " + ins.getClass());
				return result;//Return an empty result set
			}
			tmp.add(txn);
			if(!descending && count > realk)
				return tmp;
		}
		
		
		
		
		for(int i=1;i<=realk;i++)
			result.add(tmp.get(this.size() - i));
		
		return result;
		
	}

	public boolean isFull() {
		if(this.size() < this.size)
			return false;
		else
			return true;
	}
	
	
	
}
