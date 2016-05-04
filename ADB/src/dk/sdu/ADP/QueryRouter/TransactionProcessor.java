package dk.sdu.ADP.QueryRouter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.transaction.SystemException;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.util.Pair;

public class TransactionProcessor implements Runnable {
	
	private String sql;
	private HashMap<String,String> selectPlan;
	private LinkedBlockingQueue<QueryPlan> waitingQueue;
	public HashMap<String,String> processedTuples;
	//TODO: processedTuples has a known bug for tuple with multiple replica that need to have an update
	// Only the last replica will be updated, edited later!!
	private boolean returnNow;
	public boolean finished;
	private Statement stmt = null;
	public static enum TransactionType {
		NEWORDER,PAYMENT,STATUES,DELIVERY,STOCK,OTHER
	};
	ADP adp;
	TransactionType type;
	ResultSet result;
	
	
	public TransactionProcessor(ADP adp) throws SQLException{
		this(adp, TransactionType.OTHER,true);
	}
	
	public TransactionProcessor(ADP adp, TransactionType type, boolean returnNow) throws SQLException{
		sql = null;
		selectPlan = new HashMap<String,String>();
		this.waitingQueue = new LinkedBlockingQueue<QueryPlan>();
		this.type = type;
		this.returnNow = returnNow;
		this.finished = false;
		this.adp = adp;
		this.stmt = adp.conn.createStatement();
	}
	
	@Override
	public void run() {
		
		if(sql != null){
			if(sql.toUpperCase().contains("COMMIT;")){
				returnNow = true;
				finished = true;
			}
			
			QueryProcessor qp = new QueryProcessor(sql);
			
			List<QueryPlan> plan = null;
			try {
				plan = qp.processQuery(1);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			for(Iterator it = plan.iterator(); it.hasNext();){
				QueryPlan p = (QueryPlan) it.next();
				this.waitingQueue.offer(p);
			}
			//sql = null;
		}
		
		
		if(returnNow){
			try {
				result = this.finish();
				//adp.transCommit();
			} catch (SQLException e) {
				try {
					adp.transRollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				} catch (IllegalStateException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (SystemException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
				e.getNextException().printStackTrace();
			}
			//returnNow = false;

		}
		
		
		
	}

	private ResultSet finish() throws SQLException {
		if(this.processedTuples == null)
			this.processedTuples = new HashMap<String,String>();
		ResultSet rs;
		Set<String> partition = this.planner();
		
		try {
			rs = this.applyer(partition);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		//this.stmt.executeBatch();
		//this.stmt.clearBatch();
		
		//ADP.model.insertRecord(this.processedTuples);
		
		
		return rs;
	}
	
	private Set<String> planner(){
		
		Set<String> partition = new HashSet<String>();
		Set<Integer> queries = new HashSet<Integer>();
		Map<String, Set<Integer>> cover = new HashMap<String,Set<Integer>>();
		int i = 0; //Act as the query index,start from 1 to the size of quries
		//Max Coverage problem
		//Greedy Algorithm
		//1. Collect each partition's coverage
		//2. Greedily choose the partition cover the most queries until all the queries are covered

		for(Iterator it = this.waitingQueue.iterator();it.hasNext();){
			QueryPlan p = (QueryPlan) it.next();
			i++;
			
			for(Iterator pit = p.getRepNodes().iterator(); pit.hasNext(); ){
				
				String par = (String) pit.next();
				if(cover.containsKey(par)){
					cover.get(par).add(i);
				}else{
					Set<Integer> qset = new HashSet<Integer>();
					qset.add(i);
					cover.put(par, qset);
				}
			}
			
		}
		int qsize = cover.size();
		PriorityQueue<Pair> sorted_cover = new PriorityQueue<Pair>();
		
		for(Iterator it = cover.entrySet().iterator(); it.hasNext();){
			Entry e = (Entry) it.next();
			//Pair's default comparator is smaller value first, so we use qsize - actual size as the value
			Pair p = new Pair((String) e.getKey(), qsize - ((Set<Integer>) e.getValue()).size());
			sorted_cover.add(p);
		}
		
		while(!sorted_cover.isEmpty()){
			Pair p = sorted_cover.poll();
			queries.addAll(cover.get(p.getKey()));
			partition.add(p.getKey());
			if(queries.size() == i)
				break;
		}
		
		return partition;
	}
	
	private ResultSet applyer(Set<String> partition) throws SQLException, InterruptedException{
		
		ResultSet rs = null;
		
		if(this.stmt == null || this.stmt.isClosed()){
			this.stmt = adp.conn.createStatement();
		}
		
		
		while(!this.waitingQueue.isEmpty()){
			QueryPlan p = this.waitingQueue.poll(100,TimeUnit.MILLISECONDS);
			
			p.selectRep(partition);
			if(!p.isSpecial()){
				this.processedTuples.put(p.getKey(), p.getSelect());
				//TODO: this one has a known bug for tuple with multiple replica that need to have an update
				// Only the last replica will be updated, edited later!! Changed it in to Map<String, Set<String>> or similar
				adp.key = p.getKey();
				adp.select_par = p.getSelect();
				if(p.getSQL().contains("INSERT INTO")){
					adp.par_sql = p.getSQL().replaceAll("(?>-?\\d+(?:[\\./]\\d+)?[)])", "?)");
					//System.out.println(adp.par_sql);
				}else{
					adp.par_sql = p.getSQL().replaceAll(adp.partition_col + " = (\\d+)", adp.partition_col + " = ?");
				}
			}
			
			ResultSet qresult = null;
			if(p.isSpecial()){
				//For command other than normal query
				adp.key = "null";
				adp.select_par = "null";
				adp.par_sql = p.getSQL();
				this.stmt.execute(p.getSQL());
				qresult = this.stmt.getResultSet();
			}else if(p.getType() == ADP.QueryType.SELECT){
				qresult = this.stmt.executeQuery(p.getSQL());
			}else if(p.getType() == ADP.QueryType.INSERT){
				//Insert into lookup table
				
				//TODO: Temporary Disabled for Worklaod Generation
				this.stmt.executeUpdate(p.getSQL());
				//ADP.lookupTable.insert(p.getColumns(), p.getTableName(), p.getSelect());
				//Temporary Disabled for Worklaod Generation
			}else if(p.getType() == ADP.QueryType.DELETE){
				//Delete records in lookup table
				//TODO: Temporary Disabled for Worklaod Generation
				this.stmt.executeUpdate(p.getSQL());
				//ADP.lookupTable.delete(p.getKey(), p.getSelect());
				//TODO: Temporary Disabled for Worklaod Generation
			}else if(p.getType() == ADP.QueryType.UPDATE){
				//TODO: Temporary Disabled for Worklaod Generation
				this.stmt.executeUpdate(p.getSQL());	
				//TODO: Temporary Disabled for Worklaod Generation
			}else{
				//Unsupported operation
				this.stmt.execute(p.getSQL());
				qresult = this.stmt.getResultSet();
			}
			
			rs = qresult;
			
		}
		
		return rs;
	}
	
	public void setQuery(String sql){
		this.setQuery(sql, true);
	}
	
	public void setQuery(String sql, boolean returnnow) {
		this.returnNow = returnnow;
		this.sql = sql;
	}

	public ResultSet getResult() {
		return this.result;
	}

	public void clear() {
		this.processedTuples.clear();
		this.waitingQueue.clear();
		this.selectPlan.clear();
		
	}
	

}
