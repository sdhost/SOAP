package dk.sdu.ADP.QueryRouter;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import dk.sdu.ADP.ADP;
import dk.sdu.ADP.ADP.QueryType;

public class QueryProcessor{
	
	private String query_sql;
	private CCJSqlParserManager pm = null;
	//private int transaction_id;
	
	public QueryProcessor(String SQL){
		this.query_sql = SQL;
		this.pm = new CCJSqlParserManager();
		//this.transaction_id = 0;
	}
	
	private boolean isQuery(){
		String lower = query_sql.toLowerCase();
		if(lower.contains("select")||
		   lower.contains("delete")||
		   lower.contains("insert")||
		   lower.contains("update")||
		   lower.contains("replcace"))
			return true;
		else return false;
	}
	
	public List<QueryPlan> processQuery(int tranCount) throws SQLException{
		
		
		
		////////////////////////////////////////////
		// Find all possible query plan for query //
		////////////////////////////////////////////
		List<QueryPlan> plan = null;
		String edited_query = query_sql;
		QueryVisitor qp = new QueryVisitor();
		//QueryVisitor noedit_qp = new QueryVisitor();
		QueryInfo info = null;
		QueryType type = QueryType.UNKNOWN;
		net.sf.jsqlparser.statement.Statement statement = null;
		
		if(!this.isQuery()){
			info = new QueryInfo();
			info.setCommand(query_sql);
			
		}else{
		
		
			try {
				statement = pm.parse(new StringReader(query_sql));
				
				//=========================================================================
				// Parse the original SQL statement and find the column list being accessed
				// as well as the tables.
				//=========================================================================
				
				//if(query_sql.contains("item"))
				//	System.out.println(query_sql);
				
				
				if(statement instanceof Select){
					Select stmt = (Select) statement;
					info = qp.getInfo(stmt);
					if(info.getTables().size() == 1)
						type = QueryType.SELECT;
					else{
						info.setCommand(query_sql);
						type = QueryType.JOIN;
					}
					info.setCommand(query_sql);
					//TODO: Query Lookup Table and edit the Where clause, divide the statement into multiple parts if necessary
					
				}else if(statement instanceof Delete){
					Delete stmt = (Delete) statement;
					info = qp.getInfo(stmt);
					type = QueryType.DELETE;
					info.setCommand(query_sql);
					//TODO: Query Lookup Table, delete all the replications
					
				}else if(statement instanceof Insert){
					Insert stmt = (Insert) statement;
					info = qp.getInfo(stmt);
					type = QueryType.INSERT;
					info.setCommand(query_sql);
					
					//TODO: Call the workload prediction module to determine a best data node for this new tuple, also insert into the lookup table
				}else if(statement instanceof Update){
					Update stmt = (Update) statement;
					info = qp.getInfo(stmt);
					type = QueryType.UPDATE;
					info.setCommand(query_sql);
					//TODO: Edit where clause, divide it into multiple query if replication exist
					
				}else if(statement instanceof CreateTable){
					type = QueryType.CREATE;
					
					info = new QueryInfo();
					info.setCommand(query_sql);
					
					//TODO: Add new column to the table, set it as the partitioning column
				}else if (statement instanceof Drop){
					type = QueryType.DROP;
					
					info = new QueryInfo();
					info.setCommand(query_sql);
					//TODO: Also delete tuples in lookuptable
				}else if (statement instanceof Truncate){
					type = QueryType.TRUNCATE;
					
					info = new QueryInfo();
					info.setCommand(query_sql);
					//TODO: Also delete tuples in lookuptable
				}else if(statement instanceof Replace){
					//TODO: Query Lookup Table and edit the Where clause
				}
				else{
					throw new JSQLParserException();
				}
				
				
				
			
			} catch (JSQLParserException e) {
				//Debug
				//System.out.println();
				//End Debug
				e.printStackTrace();
			}
		}
		info.setType(type);
		
		//===============================================================================
		// Query the Lookup table to find the data node related to the query
		//===============================================================================
		plan = findPlan(info, tranCount);
		
		return plan;
	}
	
	
	private List<QueryPlan> findPlan( QueryInfo info, int tranCount ) throws SQLException {
		// TODO find the data nodes list that need to accessed by the query in statement
		List<QueryPlan> plan = new ArrayList<QueryPlan>();
		//Set<String> candidate_nodes = null;

		int flag = -1;
		switch(info.getType()){
			case INSERT:
				flag = 1;
				break;
			case UPDATE:
				flag = 2;
				break;
			case UNKNOWN:
			case DROP:
			case TRUNCATE:
			case CREATE:
			case JOIN:
				flag = 3;
				break;
			default:
				flag = 0;
		}
		if(flag == 3){
			QueryPlan plana = new QueryPlan(info);
			plana.setSpecial(true);
			plana.setRep(ADP.lookupTable.getAllNodes());
			
			plan.add(plana);
			
			return plan;
		}
		
		for(Iterator it = info.getTables().iterator(); it.hasNext();){
			
			String table_name = (String) it.next();

			Set<String> candidate = null;
			if(flag == 1){
				candidate = ADP.model.getInsertNodes();
			}else{
				candidate = ADP.lookupTable.get(info.getColumns(), table_name, tranCount);
			}
			
			if(candidate == null){
				//Query without primary key
				info.setType(QueryType.UNKNOWN);
				QueryPlan plana = new QueryPlan(info);
				
				plana.setSpecial(true);
				plana.setRep(ADP.lookupTable.getAllNodes());
				plan.add(plana);
				continue;
				
			}
			
			QueryInfo simpleQuery = null;
			
			simpleQuery = info.sub(table_name, ADP.lookupTable.getKeyList(table_name));
			
			if(flag != 2){
				QueryPlan plana = new QueryPlan(simpleQuery);
				
				plana.setRep(candidate);
				plan.add(plana);
			}else{
				for(Iterator pit = candidate.iterator(); pit.hasNext();){
					String par = (String) pit.next();
					Set<String> par_set = new HashSet<String>();
					par_set.add(par);
					QueryPlan plani = new QueryPlan(simpleQuery);
					plani.setRep(par_set);
					plan.add(plani);
				}
			}
			
		}
		return plan;
	}
	
	
	

}
