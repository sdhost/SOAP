import java.io.File;
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


import bitronix.tm.BitronixTransaction;
import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.resource.jdbc.PoolingDataSource;

public class JTA {

	static int tsize = 5;
	
	Map<Integer, PoolingDataSource> sources;
	BitronixTransactionManager btm;
	Map<Integer, Properties> inis;
	
	public static enum QueryType{
		SELECT,DELETE,UPDATE,INSERT,CREATE,DROP,TRUNCATE,JOIN,UNKNOWN
	}
	
	public String workdir = System.getProperty("user.home") + File.separator + "Documents"+ File.separator + "Result" + File.separator;
	
	
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
		
		if(par == -1){
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
			if(type == JTA.QueryType.SELECT){
				rs = st.executeQuery(sql);
				if(rs == null || !rs.next()){
					st.close();
					c.close();
					return null;
				}
			}else if(type == JTA.QueryType.INSERT ||
					 type == JTA.QueryType.DELETE ||
					 type == JTA.QueryType.UPDATE){
				rsc = st.executeUpdate(sql);
				if(rsc == 0){
					st.close();
					c.close();
					return null;
				}
			}else{
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
	
	
	public static Vector<Properties> genBTMTest(int par){
		Vector<Properties> props = new Vector<Properties>();
		Map<Integer, String> ips = new HashMap<Integer, String>();
		String prop_dbname = "test";
		String prop_port = "5432";
		String prop_user = "postgres";
		String prop_pass = "sdh";
		
		//TODO: Add actual ip address
		ips.put(1, "54.195.240.55");
		ips.put(2, "54.195.240.56");
		ips.put(3, "54.195.240.57");
		ips.put(4, "54.195.240.59");
		ips.put(5, "54.195.240.60");
		
		//Alternative Servers
//		ips.put(1, "54.195.220.38");
//		ips.put(2, "54.246.45.183");
//		ips.put(3, "54.73.15.67");
//		ips.put(4, "54.246.47.136");
//		ips.put(5, "54.247.142.235");
		
		//B Servers
//		ips.put(1, "54.246.51.215");
//		ips.put(2, "54.195.230.2");
//		ips.put(3, "54.73.176.58");
//		ips.put(4, "46.137.54.133");
//		ips.put(5, "54.247.154.113");
		
		//C Servers
//		ips.put(1, "54.74.9.89");
//		ips.put(2, "176.34.173.242");
//		ips.put(3, "54.217.122.65");
//		ips.put(4, "54.195.153.138");
//		ips.put(5, "46.137.50.240");
		
		//D Servers
//		ips.put(1, "54.195.113.148");
//		ips.put(2, "54.216.233.223");
//		ips.put(3, "54.195.158.230");
//		ips.put(4, "54.195.132.175");
//		ips.put(5, "54.195.158.131");
		
		//E Servers
//		ips.put(1, "54.228.114.64");
//		ips.put(2, "54.73.154.56");
//		ips.put(3, "54.73.149.247");
//		ips.put(4, "54.195.184.56");
//		ips.put(5, "46.137.52.124");
		
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