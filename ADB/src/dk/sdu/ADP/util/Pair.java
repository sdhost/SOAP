package dk.sdu.ADP.util;

public class Pair implements Comparable<Pair>{
	
	private String key;
	private int value;
	
	public Pair(String key, int value){
		this.key = key;
		this.value = value;
	}
	
	@Override
	public int compareTo(Pair o) {
		if(o == null){
			return 0;
		}
		if(this == o)
			return 0;
		int cmp = this.value - o.value;
		if(cmp > 0)
			return -1;//1 for increasing order, -1 for decreasing
		else if(cmp < 0)
			return 1;
		else
			return this.key.compareTo(o.key);
		
	}
	
	public String getKey(){
		return key;
	}
	
	public int getValue(){
		return value;
	}
	
	@Override
	public int hashCode(){
		return (key.hashCode() + value) * key.hashCode() + value;
	}
	
	public boolean equals(Object other){
		if(other instanceof Pair){
			if(this.key.equals(((Pair) other).getKey()) && this.value == ((Pair)other).getValue())
				return true;
		}
		return false;
	}
}
