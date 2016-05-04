package dk.sdu.ADP.JTA;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import javax.sql.XADataSource;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.ADP.QueryType;


import bitronix.tm.BitronixTransaction;
import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.resource.jdbc.PoolingDataSource;

public class JTA {

	Map<Integer, PoolingDataSource> sources;
	BitronixTransactionManager btm;
	Map<Integer, Properties> inis;
	
	String CLASSNAME = "org.postgresql.xa.PGXADataSource";
	int maxPoolSize = 500;
	public int rsc = 0;
	
	public JTA(Vector<Properties> ini){
		inis = new HashMap<Integer, Properties>();
		sources = new HashMap<Integer, PoolingDataSource>();
		int i = 1;
		for(Properties p: ini){
			PoolingDataSource ds = new PoolingDataSource();
			ds.setClassName(CLASSNAME);
			ds.setUniqueName("pgsql" + String.valueOf(i));
			ds.setMaxPoolSize(maxPoolSize);
			ds.setDriverProperties(p);
//			ds.getDriverProperties().setProperty("databaseName", p.getProperty("databaseName"));
//			ds.getDriverProperties().setProperty("user", p.getProperty("user"));
//			ds.getDriverProperties().setProperty("password", p.getProperty("password"));
//			ds.getDriverProperties().setProperty("serverName", "54.217.220.194");
//			ds.getDriverProperties().setProperty("portNumber", "20011");
			ds.setAllowLocalTransactions(true);
			ds.init();
			sources.put(i, ds);
			i++;
		}
		
		btm = TransactionManagerServices.getTransactionManager();
	}
		
	public BitronixTransaction getCurrentTransaction(){
		return btm.getCurrentTransaction();
	}
	
	public int getStatus() throws SystemException{
		return btm.getStatus();
	}
	
	public void begin() throws NotSupportedException, SystemException{
		btm.begin();
	}
	
	public void commit() throws SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException, SystemException{
		btm.commit();
	}
	
	public void abort() throws IllegalStateException, SecurityException, SystemException{
		btm.rollback();
	}
	
	public ResultSet query(String sql, Integer par, QueryType type) throws SQLException, InterruptedException{
		ResultSet rs = null;
		//int rsc = 0;
		
		if(par == -1){
			//TODO: for single query access more than one data source
			System.out.println(sql);
			System.out.println("Not Implemented: Single query access more than 1 data source");
		}else if(par <= 5 && par > 0 ){
			Connection c = null;
			while(c == null){
				c = sources.get(par).getConnection();
				if(c==null)
					Thread.sleep(500);
			}
			Statement st = c.createStatement();
			if(type == ADP.QueryType.SELECT){
				rs = st.executeQuery(sql);
				if(rs == null || !rs.next()){
					//System.out.println("Failed execute: " + sql);
					st.close();
					c.close();
					return null;
//					throw new SQLException();
				}
			}else if(type == ADP.QueryType.INSERT ||
					 type == ADP.QueryType.DELETE ||
					 type == ADP.QueryType.UPDATE){
				rsc = st.executeUpdate(sql);
				if(rsc == 0){
					//System.out.println("Cannot find target: " + sql);
					st.close();
					c.close();
					return null;
//					throw new SQLException();
				}
			}else{
				//TODO: Data Management Language, Drop, Create, Truncate and others not handled here
				System.out.println("Data Management Language, Drop, Create, Truncate and others not handled here");
			}
			st.close();
			c.close();
		}else{
			System.out.println("Partition: " + par + " selected");
		}
		return rs;
	}
	
	public PoolingDataSource getDS(int par){
		return this.sources.get(par);
	}
	
	public static Vector<Properties> genBTM(int par){
		Vector<Properties> props = new Vector<Properties>();
		Map<Integer, String> ips = new HashMap<Integer, String>();
		String prop_dbname = "test";
		String prop_port = "5432";
		String prop_user = "postgres";
		String prop_pass = "sdh";
		
		//TODO: Add actual ip address
		ips.put(1, "54.247.65.216");
		ips.put(2, "54.247.65.250");
		ips.put(3, "54.247.66.13");
		ips.put(4, "54.247.66.16");
		ips.put(5, "54.247.66.41");
		
		
		for(int i=1;i<=par;i++){
			Properties ini = new Properties();
			ini.setProperty("databaseName", prop_dbname);
			ini.setProperty("user", prop_user);
			ini.setProperty("password", prop_pass);
			ini.setProperty("serverName", ips.get(i));
			ini.setProperty("portNumber", prop_port);
			props.add(ini);
		}
		
		
		return props;
	}
	
	public static Vector<Properties> genBTMTest(int par){
		Vector<Properties> props = new Vector<Properties>();
		Map<Integer, String> ips = new HashMap<Integer, String>();
		String prop_dbname = "test";
		String prop_port = "5432";
		String prop_user = "postgres";
		String prop_pass = "sdh";
		
		//TODO: Add actual ip address
//		ips.put(1, "54.195.240.55");
//		ips.put(2, "54.195.240.56");
//		ips.put(3, "54.195.240.57");
//		ips.put(4, "54.195.240.59");
//		ips.put(5, "54.195.240.60");
		//A servers
//		ips.put(1, "54.246.51.131");
//		ips.put(2, "54.73.92.15");
//		ips.put(3, "54.73.172.172");
//		ips.put(4, "54.217.111.180");
//		ips.put(5, "54.195.206.190");
		//B Servers
		ips.put(1, "54.220.252.92");
		ips.put(2, "54.216.119.87");
		ips.put(3, "54.73.131.209");
		ips.put(4, "54.217.57.113");
		ips.put(5, "54.220.203.118");
		
		for(int i=1;i<=par;i++){
			Properties ini = new Properties();
			ini.setProperty("databaseName", prop_dbname);
			ini.setProperty("user", prop_user);
			ini.setProperty("password", prop_pass);
			ini.setProperty("serverName", ips.get(i));
			ini.setProperty("portNumber", prop_port);
			props.add(ini);
		}
		
		
		return props;
	}
	
	public void close(){
		for(int i = 1; i <= this.sources.size(); i++){
			sources.get(i).close();
		}
		btm.shutdown();
	}
	
	public DatabaseMetaData getMetaData(int par) throws SQLException{
		Connection conn = sources.get(par).getConnection();
		DatabaseMetaData dmd = conn.getMetaData();
		conn.close();
		return dmd;
	}
	
	public Connection getConnection(int par) throws SQLException{
		return this.sources.get(par).getConnection();
	}
	
}
