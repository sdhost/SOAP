package dk.sdu.ADP.Model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import dk.sdu.ADP.ADP;

public class TuplePosition {
	
	
	public Map<String,String> cols;
	public String table;
	
	public TuplePosition(Map<String,String> cols, String table){
		this.cols = cols;
		this.table = table;
	}
	
	public TuplePosition(String[] line_value) {
		int count = line_value.length;
		count -= 2;// ignore the tuple key and table name
		this.table = line_value[1];
		this.cols = new HashMap<String,String>();
		for(int i = 0; i < count; i+=2){
			String col = line_value[i+2];
			String val = line_value[i+3];
			cols.put(col, val);
		}
		
	}
	
	@Override
	public String toString(){
		String line = this.table + "\t";
		
		for(Entry<String,String> e:cols.entrySet()){
			line += e.getKey() + "\t" + e.getValue() + "\t";
		}
		
		
		return line;
	}

	
	
	public String getSQL(String par, String type){
		
		if(type.equalsIgnoreCase("INSERT")){
			//It's wrong to use this method
			//return this.getInsert(par);
			return null;
		}else if(type.equalsIgnoreCase("DELETE")){
			return this.getDelete(par);
		}else{
			return null;
		}
	}
	
	public String getSQLJTA(String type){
		if(type.equalsIgnoreCase("INSERT")){
			return this.getInsertJTA();
		}else if(type.equalsIgnoreCase("DELETE")){
			return this.getDeleteJTA();
		}else if(type.equalsIgnoreCase("SELECT")){
			return this.getSelectJTA();
		}else{
			return null;
		}
	}

	private String getDelete(String par) {
		String sql = "DELETE FROM " + this.table + " WHERE ";
		for(Entry<String, String> e:this.cols.entrySet()){
			sql += e.getKey() + "=" + e.getValue() + "AND ";
		}
		sql += ADP.partition_col + "=" + par;
		
		return sql;
	}
	
	private String getDeleteJTA() {
		String sql = "DELETE FROM " + this.table + " WHERE ";
		int i = 0;
		for(Entry<String, String> e:this.cols.entrySet()){
			if(i>0)
				sql += " AND ";
			i++;
			sql += e.getKey() + "=" + e.getValue();
		}
		//sql += ADP.partition_col + "=" + par;
		
		return sql;
	}
	
	private String getSelectJTA() {
		String sql = "SELECT * FROM " + this.table + " WHERE ";
		int i = 0;
		for(Entry<String, String> e:this.cols.entrySet()){
			if(i>0)
				sql += " AND ";
			i++;
			sql += e.getKey() + "=" + e.getValue();
		}
		
		return sql;
	}
	

	private String getInsertJTA() {
		String sql = "INSERT INTO " + this.table + " ";
		String col_names = "(", col_vals = "(";
		int i = 0;
		for(Entry<String, String> e:this.cols.entrySet()){
			if(i>0){
				col_names +=",";
				col_vals += ",";
			}
			i++;
			col_names += e.getKey();
			col_vals += e.getValue();
		}
		col_names += ")";
		col_vals += ")";
		sql += col_names + " VALUES " + col_vals;
		return sql;
	}
	
	private String getInsert(String par) {
		String sql = "INSERT INTO " + this.table + " ";
		String col_names = "(", col_vals = "(";
		for(Entry<String, String> e:this.cols.entrySet()){
			col_names += e.getKey()+",";
			col_vals += e.getValue()+",";
		}
		col_names += ADP.partition_col+")";
		col_vals += par+")";
		sql += col_names + " VALUES " + col_vals;
		return sql;
	}
	
	public Map<String,String> getPK(){
		return cols;
	}
	
}
