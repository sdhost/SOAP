package dk.sdu.ADP.util;

import java.util.HashSet;

public class PropertySet<T> extends HashSet<T>{
	
	public boolean write;
	public PropertySet(){
		super();
		this.write = false;
	}
	public PropertySet(boolean write){
		super();
		this.write = write;
	}
	public boolean isWrite(){
		return this.write;
	}
	public void setWrite(){
		this.write = true;
	}
	public void setWrite(boolean write) {
		this.write = write;
		
	}
}
