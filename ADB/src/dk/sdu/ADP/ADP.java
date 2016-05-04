package dk.sdu.ADP;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Vector;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import dk.sdu.ADP.JTA.JTA;
import dk.sdu.ADP.Model.LookUpTable;
import dk.sdu.ADP.Model.WorkloadModel;
import dk.sdu.ADP.QueryRouter.TransactionProcessor;
import dk.sdu.ADP.QueryRouter.TransactionProcessor.TransactionType;
import dk.sdu.ADP.util.utility;

//import org.postgresql.xa.*;




public class ADP {
	
	public Connection conn = null;
	public Statement stmt = null;
	public JTA jta = null;
	//private static CCJSqlParserManager pm = null;
	//private static Map<String,String> LookupTable;
	public static String partition_col = "Partition";
	public static LookUpTable lookupTable = null;
	public static WorkloadModel model = null;
	private boolean initialized = false;
	
	private TransactionProcessor transaction = null;
	private boolean inTransaction = false;
	private TransactionProcessor.TransactionType last = null;
	
	public String workdir = System.getProperty("user.home") + File.separator + "Documents"+ File.separator + "Result" + File.separator;
	
	public Properties ini;
	
	public static enum QueryType{
		SELECT,DELETE,UPDATE,INSERT,CREATE,DROP,TRUNCATE,JOIN,UNKNOWN
	}
	
	
	//For debug
	public String key = "null";
	public String par_sql = "";
	public String select_par = "";
	//End debug
	
	
	private static int datanodeCount;
	
	public ADP(Properties ini,int datanodeCount, String tableDefFile, int PredictionWindowSize, int PredictionTopkSize){
		transaction = null;
		if(!this.initialized)
			this.InitXC(ini, datanodeCount, tableDefFile, PredictionWindowSize, PredictionTopkSize);
	}//Constructor that using PostgresXC as backend database system
	
	public ADP(Vector<Properties> ini, String tableDefFile, int PredictionWindowSize, int PredictionTopkSize){
		transaction = null;
		if(!this.initialized)
			this.InitBTM(ini, tableDefFile, PredictionWindowSize, PredictionTopkSize);
	}//Constructor that using Bitronix Transaction Manager as backend database system
	
	private boolean InitBTM(Vector<Properties> vini,
			String tableDefFile, int PredictionWindowSize,
			int PredictionTopkSize) {
				
			ADP.datanodeCount = vini.size();
				
			//Initial the utilities
			utility.init("MD5");
				
			//Initial the workload model
			int IntervalCount = 2;//Can be modified further
			if(model == null)
				model = new WorkloadModel(IntervalCount, PredictionWindowSize / IntervalCount, PredictionTopkSize,datanodeCount);
			
			//Initial the lookup table
			if(lookupTable == null){
				lookupTable = new LookUpTable();
				lookupTable.init(datanodeCount);
				LoadTableKeys(tableDefFile);
			}
			
			
			
			//Initial the Bitronix Transaction Manager
			jta = new JTA(vini);
			
		    
		    this.initialized = true;

			return false;
	}

	private boolean InitXC(Properties ini,int datanodeCount, String tableDefFile, int PredictionWindowSize, int PredictionTopkSize){
		
		
		//The input properties ini should contain these value:
		// driver:   The JDBC driver full class name
		// conn:     The JDBC interface address for the given database
		// user:     The user name of this database
		// password: The corresponding password for user
		this.ini = ini;
		
		ADP.datanodeCount = datanodeCount;
		
		//Initial the utilities
		utility.init("MD5");
		
		//Initial the workload model
		int IntervalCount = 2;//Can be modified further
		if(model == null)
			model = new WorkloadModel(IntervalCount, PredictionWindowSize / IntervalCount, PredictionTopkSize,datanodeCount);
		
		//Initial the lookup table
		if(lookupTable == null){
			lookupTable = new LookUpTable();
			lookupTable.init(datanodeCount);
			LoadTableKeys(tableDefFile);
		}
		
		
		
		//Initial the SQL parser manager
		//pm = new CCJSqlParserManager();
		if(conn == null){
		    try {
		    	
//				if(ini.getProperty("xa") != null){
//					PGXADataSource ds = new PGXADataSource();
//					ds.setDatabaseName(ini.getProperty("db"));
//					ds.setUser(ini.getProperty("user"));
//					ds.setPassword(ini.getProperty("password"));
//					ds.setServerName(ini.getProperty("server"));
//					ds.setPortNumber(Integer.valueOf(ini.getProperty("port")));
//				}
		    	
		    	// Register jdbcDriver
				Class.forName(ini.getProperty("driver"));
				
				// Make connections
				conn = DriverManager.getConnection(ini.getProperty("conn"),
					      ini.getProperty("user"),ini.getProperty("password"));
				conn.setAutoCommit(false);
				
				// Create Statement
			    this.stmt = conn.createStatement();
				
			    
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				//transRollback();
			} catch (SQLException e) {
				e.printStackTrace();
				//transRollback();
			}
		}else{
			try {
				this.stmt = conn.createStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	    
	    this.initialized = true;

		return false;
	}
	
	private void LoadTableKeys(String tableDefFile) {
		// TODO call the lookuptable loadKeylist function to get all the table's primary key set
		
		if(tableDefFile.equalsIgnoreCase("test"))
			testLoad();
		else
			TPCCLoad();
		
	}
	
	private void TPCCLoad(){
		//Warehouse Table
		ArrayList<String> list1 = new ArrayList<String>();
		list1.add("w_id");
		ADP.lookupTable.loadKeylist("warehouse", list1);
		
		//district Table
		ArrayList<String> list2 = new ArrayList<String>();
		list2.add("d_w_id");
		list2.add("d_id");
		ADP.lookupTable.loadKeylist("district", list2);
		
		//history Table
		ArrayList<String> list3 = new ArrayList<String>();
		list3.add("h_c_id");
		list3.add("h_c_d_id");
		list3.add("h_c_w_id");
		list3.add("h_d_id");
		list3.add("h_w_id");
		//list3.add("h_date");
		//list3.add("h_amount");
		//list3.add("h_data");
		ADP.lookupTable.loadKeylist("history", list3);
		
		//oorder Table
		ArrayList<String> list4 = new ArrayList<String>();
		list4.add("o_w_id");
		list4.add("o_d_id");
		list4.add("o_id");
		ADP.lookupTable.loadKeylist("oorder", list4);
		
		//new_order Table
		ArrayList<String> list5 = new ArrayList<String>();
		list5.add("no_w_id");
		list5.add("no_d_id");
		list5.add("no_o_id");
		ADP.lookupTable.loadKeylist("new_order", list5);
		
		//order_line table
		ArrayList<String> list6 = new ArrayList<String>();
		list6.add("ol_w_id");
		list6.add("ol_d_id");
		list6.add("ol_o_id");
		list6.add("ol_number");
		ADP.lookupTable.loadKeylist("order_line", list6);
		
		//stock table
		ArrayList<String> list7 = new ArrayList<String>();
		list7.add("s_w_id");
		list7.add("s_i_id");
		ADP.lookupTable.loadKeylist("stock", list7);
		
		//item table
		ArrayList<String> list8 = new ArrayList<String>();
		list8.add("i_id");
		ADP.lookupTable.loadKeylist("item", list8);
		
		//customer table
		ArrayList<String> list9 = new ArrayList<String>();
		list9.add("c_w_id");
		list9.add("c_d_id");
		list9.add("c_id");
		ADP.lookupTable.loadKeylist("customer", list9);
		
		
	}
	
	private void testLoad(){
		ArrayList<String> list = new ArrayList<String>();
		list.add("id");
		ADP.lookupTable.loadKeylist("load", list);
	}
	
	
	public ResultSet newQuery(String sql, TransactionType type) throws SQLException{
		//ResultSet rs;
		ResultSet result = null;
		//if(type != last && last != null){
		//	result = transaction.getResult();
		//}
		
		if(!inTransaction){
			transaction = new TransactionProcessor(this,type,true);
			this.inTransaction = true;
		}
		
		transaction.setQuery(sql);
		Thread t = new Thread(transaction);
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {

			try {
				this.transRollback();
			} catch (IllegalStateException e1) {
				e1.printStackTrace();
			} catch (SecurityException e1) {
				e1.printStackTrace();
			} catch (SystemException e1) {
				e1.printStackTrace();
			}

			e.printStackTrace();
		}
		
		
		
		result = transaction.getResult();
		if(transaction.finished){
			inTransaction = false;
		}
		
		//last = type;
		
		//this.notifyAll();
		
		return result;
	}
	
	
	public ResultSet newQueryJTA(String sql, String par, QueryType type) throws SQLException, NumberFormatException, InterruptedException{
		//ResultSet rs;
		ResultSet result = this.jta.query(sql, Integer.valueOf(par), type);	
		
		
		return result;
	}
	
	
	

	public void transRollback () throws SQLException, IllegalStateException, SecurityException, SystemException {
		if(conn != null)
			conn.rollback();
		else{
			jta.abort();
		}
		//this.stmt.execute("ROLLBACK;");
		inTransaction = false;

	}

	public void transCommit () throws SQLException, SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException, SystemException {		
		
		if(conn != null)
			conn.commit();
		else{
			jta.commit();
		}
		//conn.rollback();//Do not commit to keep the data clean
		//this.stmt.execute("ROLLBACK;");
		//this.model.insertRecord(this.transaction.processedTuples);
		inTransaction = false;

	}
	
	public void transCommit (boolean flag) throws SQLException, SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException, SystemException {		
		if(!flag){
			transCommit();
			return;
		}
			
		if(conn != null)
			conn.commit();
		else{
			jta.commit();
		}
		//conn.rollback();//Do not commit to keep the data clean
		//this.model.insertRecord(this.transaction.processedTuples);
		inTransaction = false;

	}
	
	public void transBegin(TransactionType type) throws SQLException{
		inTransaction = true;
		if(this.transaction == null)
			transaction = new TransactionProcessor(this,type,false);
		else{
			this.transaction.clear();
		}
		//this.stmt.execute("BEGIN;");
	}

	public void close() throws SQLException {
		
		if(conn != null){
			stmt.close();
			conn.close();
		}
		
		jta.close();

		
	}
	
	public DatabaseMetaData getMetaData() throws SQLException{
		return conn.getMetaData();
	}
	
	public DatabaseMetaData getMetaData(int par) throws SQLException{
		return jta.getMetaData(par);
	}

	public ResultSet newQuery(String sql) throws SQLException {
		return this.newQuery(sql,TransactionType.OTHER);
		
	}
	 
	public String getRecord(){
		return this.key + "\t" + this.select_par + "\t" + this.par_sql;
	}
	
	public String getInsertNodes(){
		String par = null;
		par = ADP.model.getInsertNodes().iterator().next();
		
		return par;
	}
	
	public void setInsertNodes(String par){
		ADP.model.insertLoad(par);
	}
	
	public int getNodecount(){
		return ADP.datanodeCount;
	}
	  
}
