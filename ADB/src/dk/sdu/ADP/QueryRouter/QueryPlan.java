package dk.sdu.ADP.QueryRouter;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.ADP.QueryType;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;



public class QueryPlan {
	private QueryInfo query;
	
	private Set<String> rep;
	
	private String select;
	
	private boolean special = false;
	
	public QueryPlan(QueryInfo info){
		this.query = info;
		this.select = null;
		this.special = false;
	}
	
	
	public Set<String> getRepNodes(){
		
		return rep;
	}
	
	public Map<String,String> getColumns(){
		return this.query.getColumns();
	}
	
	public String getTableName(){
		return this.query.getTables().iterator().next();
	}
	
	public void setRep(Set<String> rep){
		this.rep = rep;
	}
	
	public boolean selectRep(Set<String> selector){
		Set<String> bakup = new HashSet<String>();
		bakup.addAll(this.rep);
		this.rep.retainAll(selector);
		if(this.rep.size() == 0){
			this.rep = bakup;
			return true;
		}else{
			Iterator it = this.rep.iterator();
			this.select = (String) it.next();
			if(!this.special)
				this.query.editQuery(this.select);
			this.rep = bakup;
			return false;
		}
			
	}
	
	public String getSelect(){
		return this.select;
	}
	
	public boolean isSelected(){
		if(this.select == null)
			return false;
		else
			return true;
	}
	
	public ADP.QueryType getType(){
		return this.query.getType();
	}


	public String getSQL() {
		return this.query.getSQL();
	}
	
	public String getKey() throws SQLException{
		return this.query.getKey();
	}
	
	public boolean isSpecial(){
		return this.special;
	}
	
	public void setSpecial(boolean flag){
		this.special = flag;
	}


	
}
