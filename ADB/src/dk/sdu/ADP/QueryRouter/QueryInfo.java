package dk.sdu.ADP.QueryRouter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.ADP.QueryType;

public class QueryInfo {
	
	private net.sf.jsqlparser.statement.Statement statement;
	private String raw_sql; 
	private String command;
	private Map<String,String> columns;
	private List<String> tables;
	
	private ADP.QueryType type;
	
	public QueryInfo(){
		this(ADP.QueryType.UNKNOWN,null);
	}
	
	public QueryInfo(ADP.QueryType type, net.sf.jsqlparser.statement.Statement stmt){
		columns = new HashMap<String,String>();
		tables = new ArrayList<String>();
		this.type = type;
		this.statement = stmt;
		this.command = null;
		this.raw_sql = null;
	}
	
	public QueryInfo(Map<String,String> cols, List<String> tabs, ADP.QueryType type, net.sf.jsqlparser.statement.Statement statement){
		this(type,statement);
		columns.putAll(cols);
		tables.addAll(tabs);
	}
	
	public void setColumns(String col_name, String col_val){
		this.columns.put(col_name,col_val);
	}
	
	public void setTable(String table){
		if(!tables.contains(table))
			this.tables.add(table);
	}
	
	public void setCommand(String cmd){
		this.command = cmd;
	}
	
	public Map<String,String> getColumns(){
		
		return columns;
	}
	
	public List<String> getTables(){
		return tables;
	}
	
	public ADP.QueryType getType(){
		return type;
	}
	
	public void setType(ADP.QueryType type){
		this.type = type;
	}
	
	public void clear(){
		
		if(columns != null)
			columns.clear();
		if(tables != null)
			tables.clear();
	}

	public QueryInfo sub(String table_name, List<String> keyList) {
		
		//TODO need to add code dealing with complex queries with multiple statements within one query
		// This method used to extract a simple query which only contact single table.
		
		return this;
	}
	
	public boolean editQuery(String partition){
		QueryVisitor editor = new QueryVisitor();
		this.raw_sql = editor.setParCol(this.statement, ADP.partition_col, partition);
		
		if(this.raw_sql.contains("?"))
			this.raw_sql = this.raw_sql.replace("?", partition);
		//Used for separate simulation that load workload from files
		
		this.columns.put(ADP.partition_col, partition);
		
		if(raw_sql == null)
			return true;
		else
			return false;
	}
	
	public String getSQL(){
		if(this.type == QueryType.UNKNOWN || this.type == QueryType.JOIN)
			return this.command;
		else
			return this.raw_sql;
	}
	
	public String getKey() throws SQLException{
		String table_name;
		if(this.tables.size() != 1){
			System.out.println("Unsupported multiple tables in query");
			return null;
		}else{
			table_name = this.tables.get(0);
			return ADP.lookupTable.getKey(this.columns, table_name);
		}
		
	}
	
}
