package dk.sdu.ADP.debug;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import pojo.Warehouse;
import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.resource.jdbc.PoolingDataSource;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.ADP.QueryType;
import dk.sdu.ADP.JTA.JTA;
import dk.sdu.ADP.QueryRouter.QueryPlan;
import dk.sdu.ADP.QueryRouter.QueryProcessor;
import dk.sdu.ADP.Repartition.RepartitionApplier;
import dk.sdu.ADP.Repartition.RepartitionSearcher;
import dk.sdu.ADP.util.Pair;
import dk.sdu.ADP.util.PropertySet;


public class debug {

	static ADP adp;
	static LinkedList<String> history;
	static Map<String,LinkedList<String>> txnDef;
	static Map<String, Vector<String>> txnPar;
	static LinkedList<String> replist = null;
	static LinkedList<Integer> load_dist = null;
	
	static private FileOutputStream out;
	static private PrintStream pout;
	
	static private int thresh_memo = 30000;
	static private int thresh_rep = 120000;
	static private double thresh_cost = 0.0;//Apply all the new design
	static private Executioner[] e;
	static private Repartitioner[] reppro;
	
	static private RepartitionSearcher rs = null;
	
	static private String workload_file = null;
	static private String workload_file_par = null;
	static private String workload_file_nopar = null;
	static private String addr_lookup = null;
	static private String poissdst = null;
	static private boolean ispar = false;
	static private int warehouse_tot = 0;
	
	
	static private PoolingDataSource ds = null;
	static private BitronixTransactionManager btm = null;
	
	//JDBC properties for each partition
	static private Map<Integer, String> ips = null;
	static private String prop_dbname = "test_1";
	static private String prop_port = "20011";
	static private String prop_user = "postgres";
	static private String prop_pass = "sdh";
	
	
	public static void main(String[] args) {
		
		
		
//		workload_file = "Test1WorkloadZipf@138909394";
//		poissdst = "PoissonRND";
//		workload_file_par = "Test1Workload.Repartition@138909394";
		

//		Simulator sim = new Simulator();
//		sim.setAmount(50);
//		sim.Start(workload_file, workload_file_par, poissdst, 1);
//
//		System.out.println("Finished");

		Test1Load load = new Test1Load(0.5, 5);
		load.GenRandom();
		//load.GenZipf(System.getProperty("user.home") + "/Documents/Result/" + "ZipfRND2");
		
		

	}
	
	
	
	
	public static void singlePartitioned(int repeat) throws IllegalStateException, SecurityException, SystemException{
		String sql1 = "UPDATE warehouse SET w_ytd=w_ytd + '1380.92' WHERE w_id = '3'";
		String sql2 = "UPDATE district SET d_ytd=d_ytd + '1380.92' WHERE d_w_id = '1' AND d_id = '3'";
		String sql3 = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = '3' AND c_d_id = '1' AND c_id = '2'";
		String sql4 = "UPDATE warehouse SET w_ytd=w_ytd + '0.1' where w_id = '8'";
		String sql5 = "UPDATE district SET d_ytd=d_ytd + '0.1' where d_w_id = '1' AND d_id = '8'";
		
		
		//long before = System.currentTimeMillis();
		
		try {
//			PreparedStatement ps1 = adp.conn.prepareStatement(sql1);
//			PreparedStatement ps2 = adp.conn.prepareStatement(sql2);
//			PreparedStatement ps3 = adp.conn.prepareStatement(sql3);
			PoolingDataSource ds = adp.jta.getDS(1);
			//ds.init();
			Connection conn = ds.getConnection();
			conn.setAutoCommit(false);
			PreparedStatement ps1 = conn.prepareStatement(sql1);
			PreparedStatement ps2 = conn.prepareStatement(sql2);
			PreparedStatement ps3 = conn.prepareStatement(sql3);
			PreparedStatement ps4 = conn.prepareStatement(sql4);
			PreparedStatement ps5 = conn.prepareStatement(sql5);
			
			
			//for(int i = 0; i< repeat; i++){
//				adp.stmt.execute("BEGIN");
				adp.jta.begin();
				//ps1.setInt(1, 1);
				int j = ps1.executeUpdate();
				if(j==0){
					System.out.println("Query1 Failed");
					//adp.transRollback();
					adp.jta.abort();
					//continue;
				}
				
				//ps2.setInt(1, 1);
				j = ps2.executeUpdate();
				if(j==0){
					System.out.println("Query2 Failed");
					//adp.transRollback();
					adp.jta.abort();
					//continue;
				}
				
				//ps3.setInt(1, 1);
				ResultSet rs = ps3.executeQuery();
				if(rs==null || !rs.next()){
					System.out.println("Query3 Failed");
//					adp.transRollback();
					adp.jta.abort();
					//continue;
				}
				
				j = ps4.executeUpdate();
				if(j==0){
					System.out.println("Query4 Failed");
//					adp.transRollback();
					adp.jta.abort();
					//continue;
				}
				
				j = ps5.executeUpdate();
				if(j==0){
					System.out.println("Query5 Failed");
//					adp.transRollback();
					adp.jta.abort();
					//continue;
				}
				ps1.close();
				ps2.close();
				ps3.close();
				ps4.close();
				ps5.close();
				conn.close();
				//ds.close();
//				adp.transCommit();
				adp.jta.commit();
				
				//if(i % 100 == 0){
				//	System.out.print(i + "/" + repeat + "\t");
				//}
				
			//}
			
		} catch (SQLException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (NotSupportedException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (SystemException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (SecurityException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (IllegalStateException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (RollbackException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (HeuristicMixedException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (HeuristicRollbackException e) {
			e.printStackTrace();
			adp.jta.abort();
		}
		

		//long after = System.currentTimeMillis();
		
		//System.out.println("Throughput: " + String.valueOf((double)repeat / (double)(after - before) * 60000.0));

		
	}
	
	public static void allPartitioned(int repeat) throws IllegalStateException, SecurityException, SystemException{
		String sql1 = "UPDATE warehouse SET w_ytd=w_ytd + '4043.16' WHERE w_id = '8'";
		String sql2 = "UPDATE district SET d_ytd=d_ytd + '4043.16' WHERE d_w_id = '1' AND d_id = '7'";
		String sql3 = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = '3' AND c_d_id = '1' AND c_id = '1546'";
		String sql4 = "UPDATE warehouse SET w_ytd=w_ytd + '0.1'  where w_id = '5'";
		String sql5 = "UPDATE district SET d_ytd=d_ytd + '0.1' where d_w_id = '1' AND d_id = '9'";
		
//		long before = System.currentTimeMillis();
		
		try {
//			PreparedStatement ps1 = adp.conn.prepareStatement(sql1);
//			PreparedStatement ps2 = adp.conn.prepareStatement(sql2);
//			PreparedStatement ps3 = adp.conn.prepareStatement(sql3);
			PoolingDataSource ds1 = adp.jta.getDS(1);
			PoolingDataSource ds2 = adp.jta.getDS(2);
			PoolingDataSource ds3 = adp.jta.getDS(3);
			PoolingDataSource ds4 = adp.jta.getDS(4);
			PoolingDataSource ds5 = adp.jta.getDS(5);
//			ds1.init();
//			ds2.init();
//			ds3.init();
//			ds4.init();
//			ds5.init();
			Connection conn1 = ds1.getConnection();
			Connection conn2 = ds2.getConnection();
			Connection conn3 = ds3.getConnection();
			Connection conn4 = ds4.getConnection();
			Connection conn5 = ds5.getConnection();
			conn1.setAutoCommit(false);
			conn2.setAutoCommit(false);
			conn3.setAutoCommit(false);
			conn4.setAutoCommit(false);
			conn5.setAutoCommit(false);
			PreparedStatement ps1 = conn1.prepareStatement(sql1);
			PreparedStatement ps2 = conn2.prepareStatement(sql2);
			PreparedStatement ps3 = conn3.prepareStatement(sql3);
			PreparedStatement ps4 = conn4.prepareStatement(sql4);
			PreparedStatement ps5 = conn5.prepareStatement(sql5);
			
//			for(int i = 0; i< repeat; i++){
//				adp.stmt.execute("BEGIN");
				adp.jta.begin();
				int j = ps1.executeUpdate();
				if(j==0){
					System.out.println("Query1 Failed");
					//adp.transRollback();
					adp.jta.abort();
					//continue;
				}
				
				//ps2.setInt(1, 1);
				j = ps2.executeUpdate();
				if(j==0){
					System.out.println("Query2 Failed");
					//adp.transRollback();
					adp.jta.abort();
					//continue;
				}
				
				//ps3.setInt(1, 1);
				ResultSet rs = ps3.executeQuery();
				if(rs==null || !rs.next()){
					System.out.println("Query3 Failed");
//					adp.transRollback();
					adp.jta.abort();
					//continue;
				}
				
				j = ps4.executeUpdate();
				if(j==0){
					System.out.println("Query4 Failed");
//					adp.transRollback();
					adp.jta.abort();
					//continue;
				}
				
				j = ps5.executeUpdate();
				if(j==0){
					System.out.println("Query5 Failed");
//					adp.transRollback();
					adp.jta.abort();
					//continue;
				}
//				adp.transCommit();
				ps1.close();
				ps2.close();
				ps3.close();
				ps4.close();
				ps5.close();
				conn1.close();
				conn2.close();
				conn3.close();
				conn4.close();
				conn5.close();
//				ds1.close();
//				ds2.close();
//				ds3.close();
//				ds4.close();
//				ds5.close();
				
				adp.jta.commit();
				
//				if(i % 100 == 0){
//					System.out.print(i + "/" + repeat + "\t");
//				}
				
//			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (NotSupportedException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (SystemException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (SecurityException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (IllegalStateException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (RollbackException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (HeuristicMixedException e) {
			e.printStackTrace();
			adp.jta.abort();
		} catch (HeuristicRollbackException e) {
			e.printStackTrace();
			adp.jta.abort();
		}
		

//		long after = System.currentTimeMillis();
		
//		System.out.println("Throughput: " + String.valueOf((double)repeat / (double)(after - before) * 60000.0));

		
	}
	
//	public static void allPartitioned(int repeat){
//		String sql1 = "UPDATE warehouse SET w_ytd=w_ytd + '4043.16' WHERE w_id = '13' AND Partition = ?";
//		String sql2 = "UPDATE district SET d_ytd=d_ytd + '4043.16' WHERE d_w_id = '13' AND Partition = ? AND d_id = '9'";
//		String sql3 = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = '13' AND c_d_id = '9' AND Partition = ? AND c_id = '1371'";
//		
//		long before = System.currentTimeMillis();
//		
//		try {
//			PreparedStatement ps1 = adp.conn.prepareStatement(sql1);
//			PreparedStatement ps2 = adp.conn.prepareStatement(sql2);
//			PreparedStatement ps3 = adp.conn.prepareStatement(sql3);
//			
//			for(int i = 0; i< repeat; i++){
//				adp.stmt.execute("BEGIN");
//				ps1.setInt(1, 3);
//				int j = ps1.executeUpdate();
//				if(j==0){
//					System.out.println("Query1 Failed");
//					adp.transRollback();
//					continue;
//				}
//				
//				ps2.setInt(1, 2);
//				j = ps2.executeUpdate();
//				if(j==0){
//					System.out.println("Query2 Failed");
//					adp.transRollback();
//					continue;
//				}
//				
//				ps3.setInt(1, 1);
//				ResultSet rs = ps3.executeQuery();
//				if(rs==null || !rs.next()){
//					System.out.println("Query3 Failed");
//					adp.transRollback();
//					continue;
//				}
//				adp.transCommit();
//				
//				if(i % 1000 == 0){
//					System.out.print(i + "/" + repeat + "\t");
//				}
//			}
//			
//		} catch (SQLException e) {
//
//			e.printStackTrace();
//		}
//		
//
//		long after = System.currentTimeMillis();
//		
//		System.out.println("Throughput: " + String.valueOf((double)repeat / (double)(after - before) * 60000.0));
//
//		
//	}
	
	public static void debugRepartitionJTA() throws IOException{
		workload_file = "10.0t1@137959729";
		boolean exec = false;//Switch for whether need repartition
		ispar = false;//Switch for whether workload is partitioned
		int terminal_count = 20;
		int start = 0;
		try {
			start = initJTA(terminal_count,exec);
		} catch (SQLException e) {
			e.printStackTrace();
		} //1,2,3
		
		//testAvgQueries();
		
		if(exec)
			execJTA(terminal_count);
		else
			simJTA(1, start);
		// 4
		
		store();// 5
		
		
		
	}
	
	
	public static void debugRepartition() throws IOException{
		//Todo: 1. Initial the ADP main class
		//		2. Load the LookUpTable
		//		3. Load Predefined Transactions
		//		4. Simulate the transaction processing, do the repartitioning and add new replicas in the system
		//		5. Save the LookUpTable again and the processed workload traces
		//
		//
		warehouse_tot = 20;
		workload_file = "P50NO50_20Warehouse_30min";
		workload_file_par = "P50NO50_20Warehouse_30min.par";
		workload_file_nopar = "P50NO50_20Warehouse_30min.nopar";
		addr_lookup = "lookuptable.ec2.dump";
		boolean exec = false;//Switch for whether need repartition
		ispar = true;//Switch for whether workload is partitioned
		int terminal_count = 20;
		int start = 0;
		try {
			start = initXC(terminal_count,exec);
		} catch (SQLException e) {
			e.printStackTrace();
		} //1,2,3
		
		//testAvgQueries();
		
		if(exec)
			execXC(terminal_count);
		else
			simXC(5, start);
		// 4
		
		store();// 5
		
		
		
	}
	
	private static void execTest(int numThread, int numRept) throws FileNotFoundException, SQLException{
		Map<Integer,Boolean> repfinish = new HashMap<Integer, Boolean>();
		File f = new File(adp.workdir + "ProcessedWorkload");
		if(f.exists()){
			f.delete();
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		FileOutputStream performance_out = new FileOutputStream(f);
		PrintStream thro = new PrintStream(performance_out);
		
		thro.println("TPMC");
		
		long current = System.currentTimeMillis();
		Thread t[] = new Thread[numThread];
		Thread trep[] = new Thread[numRept];
		
		int step_rep = replist.size() / numRept;
		int last_total = 0;
		
		for(int i=0;i<numRept;i++){
			reppro[i] = new Repartitioner( replist.subList(last_total, (last_total+step_rep)), step_rep, adp.jta);
			reppro[i].setStart(0);
			repfinish.put(i, false);
			last_total += step_rep;
			
			
			trep[i] = new Thread(reppro[i]);
			
		}
		for(int i=0; i < numThread; i++){
			String name = "Thread";
			name = name + String.valueOf(i);
			//e[i] = new Executioner(name, history, txnDef, adp.jta, 0.0);
			e[i] = new Executioner(name, history, adp.jta, 0.5);
			t[i] = new Thread(e[i]);
		}
		int txn_count = 0;
		float max = 17;
		int normal_interval = 20;
		int finish_interval = 20;
		for(Integer load: load_dist){
			normal_interval--;
			int scale_load = load * 2;
			float scale;
			if(scale_load > max)
				scale = 1;
			else
				scale = scale_load / max;
			long start = System.currentTimeMillis();
			long deadlinet = (int)Math.floor(60000 * scale) - 500;
			long deadline = 60000 - (int)Math.floor(60000 * scale) - 500;
			double progress = 0;
			for(int i=0;i<numRept;i++){
				double prog = reppro[i].getProgress();
				if(prog >= 1){
					repfinish.put(i, true);
					prog = 1;
					reppro[i].stopThread();
				}
				System.out.print(prog + "\t");
				progress += prog;
				
			}
			System.out.println();
			progress /= numRept;
			if(progress == 1.0d){
				finish_interval--;
			}
			System.out.println("RepProgress: " + progress + "\t Execute: " + deadlinet);
			for(int i=0;i<numThread;i++){
				e[i].setRepRate(progress);
				e[i].setInterval(deadlinet);
				e[i].stop = false;
				if(t[i].getState() == Thread.State.NEW)
					t[i].start();
				else{
					synchronized(e[i]){
						e[i].notify();
					}
				}
					
			}
			
			try {
				Thread.sleep(deadlinet);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			for(int i=0;i<numThread;i++){
				if(!e[i].isStopped())
					e[i].stopThread();
			}
			if(normal_interval > 0 && deadline > 0){
				System.out.println("Repartition begin in: " + normal_interval + " minutes");
				try {
					Thread.sleep(deadline);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else if(deadline > 0){
				for(int i=0;i<numRept;i++)
					//For feedback solution
					if(trep[i].getState() == Thread.State.NEW){
						reppro[i].setInterval(100);
						trep[i].start();
					}
					else{
						synchronized(reppro[i]){
							reppro[i].setInterval(100);
							reppro[i].notify();
						}
					}
				if(deadline > 0)
					try {
						Thread.sleep(deadline);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					//End Feedback solution
			}
			//******************************
			//     Code for determinate optimization solution
//			else if(deadline > 0){
//				System.out.println("Repartition Start");
//				for(int i=0;i<numRept;i++){
//					if(repfinish.get(i))
//						continue;
//					reppro[i].stop = false;
//					if(trep[i].getState() == Thread.State.NEW)
//						trep[i].start();
//					else{
//						synchronized(reppro[i]){
//							reppro[i].notify();
//						}
//					}
//				}
//				try {
//					Thread.sleep(deadline);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//				for(int i=0;i<numRept;i++)
//					reppro[i].stopThread();
//			}
			//****************************************
			//Code for feedback solution
			
			
			int txn_now = 0;
			for(int i=0;i<numThread;i++){
				//if(e[i].isStopped()){
					txn_now += e[i].getCount();
					System.out.print(e[i].getCount() + "\t");
				//}
			}
			System.out.println();
			System.out.println("Throughput: " + String.valueOf(txn_now - txn_count));
			System.out.println("Latency: " + String.valueOf((double)deadlinet / (double)(txn_now - txn_count)));
			thro.println(String.valueOf(txn_now - txn_count) + "\t" + scale + "\t" + (double)deadlinet / (double)(txn_now - txn_count));
			txn_count = txn_now;
			
			if(finish_interval < 0)
				break;
			
			
		}
		
		thro.close();
		try {
			performance_out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void execJTA(int numThread) throws FileNotFoundException{
		File f = new File(adp.workdir + "ProcessedWorkload");
		if(f.exists()){
			f.delete();
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		FileOutputStream performance_out = new FileOutputStream(f);
		PrintStream thro = new PrintStream(performance_out);
		
		thro.println("TPMC");
		
		long current = System.currentTimeMillis();
		for(int i=0; i < numThread; i++){
			String name = "Thread";
			name = name + String.valueOf(i);
			try {
				e[i] = new Executioner(name, history, txnDef, adp.jta, replist.size());
				Thread t = new Thread(e[i]);
				t.start();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		int txn_count = 0;
		while(true){
			try {
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			boolean stopped = true;
			int txn_now = 0;
			for(int i =0; i < numThread; i++){
				if(stopped && !e[i].isStopped()){
					stopped = false;
				}
				if(e[i].isStopped())
					System.out.print("S");
				System.out.print(e[i].getCount() + "P" + e[i].repc + "\\" + e[i].rep + "\t");
				txn_now += e[i].getCount();
			}
			System.out.println(txn_now - txn_count);
			thro.println(txn_now - txn_count);
			txn_count = txn_now;
			if(stopped){
				break;
			}
		}
		
		thro.close();
		try {
			performance_out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void execXC(int numThread) throws FileNotFoundException{
		File f = new File(adp.workdir + "ProcessedWorkload");
		if(f.exists()){
			f.delete();
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		FileOutputStream performance_out = new FileOutputStream(f);
		PrintStream thro = new PrintStream(performance_out);
		
		thro.println("TPMC");
		
		long current = System.currentTimeMillis();
		for(int i=0; i < numThread; i++){
			String name = "Thread";
			name = name + String.valueOf(i);
			try {
				e[i] = new Executioner(name, history, txnDef, adp.conn);
				Thread t = new Thread(e[i]);
				t.start();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		int txn_count = 0;
		while(true){
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			boolean stopped = true;
			int txn_now = 0;
			for(int i =0; i < numThread; i++){
				if(stopped && !e[i].isStopped()){
					stopped = false;
				}
				System.out.print(e[i].getCount() + "\t");
				txn_now += e[i].getCount();
			}
			System.out.println(txn_now - txn_count);
			thro.println(txn_now - txn_count);
			txn_count = txn_now;
			if(stopped){
				break;
			}
		}
		
		thro.close();
		try {
			performance_out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private static void simJTA(int repeat, int startCnt) throws IOException {
		int transactionCount = startCnt, lastCount = startCnt;
		int singleCount = 0, lastSingle = 0;
		File f = new File(adp.workdir + "ProcessedWorkload");
		if(f.exists()){
			f.delete();
			f.createNewFile();
		}
		
		FileOutputStream performance_out = new FileOutputStream(f);
		PrintStream thro = new PrintStream(performance_out);
		//thro.println("tpmC\tTraC\tITraC\tILenh\tSingle\tSearch\tApply\tQuery\tDC\tSkew");
		long start = System.currentTimeMillis(), lastmemo = start, lastsearch = start;
		//String transactionTypeName = null;
		double cDesign = Double.MAX_VALUE; 
		
		
		//boolean error = false;
		
		for(int i =0; i < repeat; i++){
			
			for(String txni:history){
				//txni: key of current transaction
				boolean single = true;
				String lastPar = null;
				transactionCount++;
				String record = "Begin\n";
				boolean predefined = false;
				boolean error = false;
				Map<String, PropertySet<String>> routing = ADP.model.getRouting(txni);
				if(routing != null)
					predefined = true;
				else
					routing = new HashMap<String,PropertySet<String>>();
				
				try {
					adp.jta.begin();
				}catch (NotSupportedException e) {
					e.printStackTrace();
				} catch (SystemException e) {
					e.printStackTrace();
				}
				
				for(String query: txnDef.get(txni)){
					String[] elem = query.split("\t");
					String key = elem[0];
					query = elem[1];
       
					//Begin key information retrieval and partition select
    				String table;
    				Map<String,String> columns = new HashMap<String,String>();
        			QueryProcessor qp = new QueryProcessor(query);
        			List<QueryPlan> plans = new ArrayList<QueryPlan>();
					try {
						plans = qp.processQuery(transactionCount);
					} catch (SQLException e) {
						e.printStackTrace();
					}
        			PropertySet<String> partition = null;
        			
        			boolean write = false;
        			if(query.contains("UPDATE"))
        				write = true;
        			
        			if(plans.isEmpty()){
        				error = true;
        				System.out.println("Error processing query: " + query);
        				break; // Error in transaction
        			}
        			
        			//key = plans.iterator().next().getKey();
    				columns = plans.iterator().next().getColumns();
    				table = plans.iterator().next().getTableName();
    				
    				if(columns.containsKey(ADP.partition_col))
    					columns.remove(ADP.partition_col);
    				if(ADP.lookupTable.getRaw(key) == null)
    					ADP.lookupTable.updateRaw(key, columns, table);
        			
    				if(predefined){
    					partition = routing.get(key);
    					partition.setWrite(write);
    				}else{
    					try {
							partition = LookUpQuery(key, write, transactionCount);
						} catch (SQLException e) {
							e.printStackTrace();
						}
    					if(partition == null){
    						error = true;
    						System.out.println("Cannot find target in LookupTable: " + query);
    						break;
    					}
    						
    					routing.put(key, partition);
    				}
    				
    				//End key information retrieval and partition select
    				
    				//Begin Query Execution
					if(partition.isWrite()){
						for(String par:partition){
							String sql = query;
							if(!sql.toLowerCase().contains("update"))
								System.out.println(sql);
							try {
								adp.newQueryJTA(sql, par, QueryType.UPDATE);
							} catch (SQLException e) {
								e.printStackTrace();
							} catch (NumberFormatException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							if(lastPar == null)
								lastPar = par;
							else if(single && lastPar != par)
								single = false;
							lastPar = par;
//    							if(rsc == 0){
//    								error = true;
//    								System.out.println("Cannot find target: " + sql);
//    								break;
//    							}else{
//    								//System.out.println(rsc + " tuple Updated");
//    							}
						}
						record += key + "\t" + lastPar + "\t" + query + "\n"; 
					}else{
						String par = partition.iterator().next();
						
						
						if(query.toLowerCase().contains("select")){
							String sql = query.replace("?", par);
							try {
								ResultSet result = adp.newQueryJTA(sql, par, QueryType.SELECT);
//								if(!result.next()){
//									error = true;
//									System.out.println("Cannot find target: " + sql);
//									break;
//								}
							} catch (SQLException e) {
								e.printStackTrace();
							} catch (NumberFormatException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							if(lastPar == null)
								lastPar = par;
							else if(single && lastPar != par)
								single = false;
							lastPar = par;
							
						}else{
							//For insert
//							String sql;
//							if(lastPar == null){
//								lastPar = par;
//								
//							}
//							sql = query.replace("?", lastPar);
//							int rsc = 0;
//							try {
//								rsc = adp.stmt.executeUpdate(sql);
//							} catch (SQLException e) {
//								e.printStackTrace();
//							}//Comment For virtual Executing Query
//							
//							
//							
//							if(rsc == 0){
//								error = true;
//								System.out.println("Cannot find target: " + sql);
//								//break;
//							}else{
//								//System.out.println(rsc + " tuple Inserted");
//								ADP.lookupTable.insert(columns, table, par);
//							}
//							
//						}
//						
//						record += key + "\t" + lastPar + "\t" + query + "\n"; 
					}
        			
        			

				}
				}
            record += "COMMIT\n";	
			try {
				adp.transCommit();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (IllegalStateException e) {
				e.printStackTrace();
			} catch (RollbackException e) {
				e.printStackTrace();
			} catch (HeuristicMixedException e) {
				e.printStackTrace();
			} catch (HeuristicRollbackException e) {
				e.printStackTrace();
			} catch (SystemException e) {
				e.printStackTrace();
			}
			if(error){
				System.out.println("Error exist!");
				continue;
			}
			
			pout.print(record);
			
			if(single)
				singleCount++;
			
			UpdateWorkloadModel(routing, predefined, txni);
			boolean printed = false;
			long CurrentMilis = System.currentTimeMillis();
			if(CurrentMilis - lastmemo >= thresh_memo){
				double tpmC = 6000000.0 * (double)(transactionCount - lastCount) / (double)(CurrentMilis - lastmemo) / 100.0;
				//thro.print(String.format("%.2e", tpmC) + "\t" + String.valueOf(transactionCount) + "\t" + String.valueOf(transactionCount - lastCount) + "\t" + String.valueOf(CurrentMilis - lastmemo));
				lastmemo = CurrentMilis;
				lastCount = transactionCount;
				System.out.println("Processed: " + transactionCount + "/" + repeat * history.size());
				
				//thro.print("\t" + String.valueOf(singleCount - lastSingle));
				lastSingle = singleCount;
				
				if(CurrentMilis - lastsearch >= thresh_rep){
					long beforeSearch = System.currentTimeMillis();
					cDesign = ADP.model.getDesignCost();
					double design = RepartitionSearch(transactionCount);
					System.out.println("Searched Plan: " + design + "\t" + "Current Plan:" + cDesign);
					long afterSearch = System.currentTimeMillis();
					int repCount = 0;
					if(1 - design/cDesign >= thresh_cost){
						Vector<String> queries = RepartitionQueries();
						if(queries != null){
							for(String q:queries){
								thro.println(q);
							}
							thro.println();
						}
						cDesign = rs.last_cost;
						rs.reset_design();
					}
					long afterapply = System.currentTimeMillis();
					lastsearch = afterapply;
					lastmemo = afterapply;
					
//					thro.print("\t" + String.valueOf(afterSearch - beforeSearch)
//							+ "\t" + String.valueOf(afterapply - afterSearch)
//							+ "\t" + String.valueOf(repCount)
//							+ "\t" + String.format("%.2e", adp.model.getDistCost())
//							+ "\t" + String.format("%.2e", adp.model.getSkew()));
					
				}// Search for new design and check if it is time to do a repartitioning
				else{//Print some space position holder
					//thro.print("\t \t \t \t \t ");
				}
				
				
				
				printed = true;
			}//Store the throughput
			
			if(printed){
				thro.print("\n");
			}
		}
			
		}
		thro.close();
		performance_out.close();
	}

	
	
	
	
	private static void simXC(int repeat, int startCnt) throws IOException {
		int transactionCount = startCnt, lastCount = startCnt;
		int singleCount = 0, lastSingle = 0;
		File f = new File(adp.workdir + "ProcessedWorkload");
		if(f.exists()){
			f.delete();
			f.createNewFile();
		}
		
		FileOutputStream performance_out = new FileOutputStream(f);
		PrintStream thro = new PrintStream(performance_out);
		thro.println("tpmC\tTraC\tITraC\tILenh\tSingle\tSearch\tApply\tQuery\tDC\tSkew");
		long start = System.currentTimeMillis(), lastmemo = start, lastsearch = start;
		//String transactionTypeName = null;
		double cDesign = Double.MAX_VALUE; 
		
		
		//boolean error = false;
		
		for(int i =0; i < repeat; i++){
			
			for(String txni:history){
				//txni: key of current transaction
				boolean single = true;
				String lastPar = null;
				transactionCount++;
				String record = "Begin\n";
				boolean predefined = false;
				boolean error = false;
				Map<String, PropertySet<String>> routing = ADP.model.getRouting(txni);
				if(routing != null)
					predefined = true;
				else
					routing = new HashMap<String,PropertySet<String>>();
				
				try {
					adp.stmt.execute("BEGIN");
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				
				for(String query: txnDef.get(txni)){
					String[] elem = query.split("\t");
					String key = elem[0];
					query = elem[1];
        			if(query.contains(ADP.partition_col)){     
        				String table;
        				Map<String,String> columns = new HashMap<String,String>();
            			QueryProcessor qp = new QueryProcessor(query);
            			List<QueryPlan> plans = new ArrayList<QueryPlan>();
						try {
							plans = qp.processQuery(transactionCount);
						} catch (SQLException e) {
							e.printStackTrace();
						}
            			PropertySet<String> partition = null;
            			
            			boolean write = false;
            			if(query.contains("UPDATE"))
            				write = true;
            			
            			if(plans.isEmpty()){
            				error = true;
            				System.out.println("Error processing query: " + query);
            				break; // Error in transaction
            			}
            			
            			//key = plans.iterator().next().getKey();
        				columns = plans.iterator().next().getColumns();
        				table = plans.iterator().next().getTableName();
        				
        				if(columns.containsKey(ADP.partition_col))
        					columns.remove(ADP.partition_col);
        				if(ADP.lookupTable.getRaw(key) == null)
        					ADP.lookupTable.updateRaw(key, columns, table);
            			
        				if(predefined){
        					partition = routing.get(key);
        					partition.setWrite(write);
        				}else{
        					try {
								partition = LookUpQuery(key, write, transactionCount);
							} catch (SQLException e) {
								e.printStackTrace();
							}
        					if(partition == null){
        						error = true;
        						System.out.println("Cannot find target in LookupTable: " + query);
        						break;
        					}
        						
        					routing.put(key, partition);
        				}
        				
    					if(partition.isWrite()){
    						for(String par:partition){
    							String sql = query.replace("?", par);
    							if(!sql.toLowerCase().contains("update"))
    								System.out.println(sql);
    							try {
									int rsc = adp.stmt.executeUpdate(sql);
								} catch (SQLException e) {
									e.printStackTrace();
								}
    							if(lastPar == null)
    								lastPar = par;
    							else if(single && lastPar != par)
    								single = false;
    							lastPar = par;
//    							if(rsc == 0){
//    								error = true;
//    								System.out.println("Cannot find target: " + sql);
//    								break;
//    							}else{
//    								//System.out.println(rsc + " tuple Updated");
//    							}
    						}
    						record += key + "\t" + lastPar + "\t" + query + "\n"; 
    					}else{
    						String par = partition.iterator().next();
    						
    						
    						if(query.toLowerCase().contains("select")){
    							String sql = query.replace("?", par);
    							try {
									ResultSet result = adp.stmt.executeQuery(sql);
								} catch (SQLException e) {
									e.printStackTrace();
								}
    							if(lastPar == null)
    								lastPar = par;
    							else if(single && lastPar != par)
    								single = false;
    							lastPar = par;
//								if(!result.next()){
//									//error = true;
//									//System.out.println("Cannot find target: " + sql);
//									//break;
//								}
    						}else{
    							//For insert
    							String sql;
    							if(lastPar == null){
    								lastPar = par;
    								
    							}
    							sql = query.replace("?", lastPar);
    							int rsc = 0;
								try {
									rsc = adp.stmt.executeUpdate(sql);
								} catch (SQLException e) {
									e.printStackTrace();
								}//Comment For virtual Executing Query
    							
    							
    							
    							if(rsc == 0){
    								error = true;
    								System.out.println("Cannot find target: " + sql);
    								//break;
    							}else{
    								//System.out.println(rsc + " tuple Inserted");
    								ADP.lookupTable.insert(columns, table, par);
    							}
    							
    						}
							
							record += key + "\t" + lastPar + "\t" + query + "\n"; 
    					}
        			}else{
        				ResultSet result = null;
						try {
							result = adp.stmt.executeQuery(query);
						} catch (SQLException e) {
							e.printStackTrace();
						}
						try {
							if(result == null || !result.next()){
								error = true;
								System.out.println("Failed execute: " + query);
								break;
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
						record += "null" + "\t" + "null" + "\t" + query + "\n"; 
        			}
        			
        			

				}
            record += "COMMIT\n";	
			try {
				adp.transCommit();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (IllegalStateException e) {
				e.printStackTrace();
			} catch (RollbackException e) {
				e.printStackTrace();
			} catch (HeuristicMixedException e) {
				e.printStackTrace();
			} catch (HeuristicRollbackException e) {
				e.printStackTrace();
			} catch (SystemException e) {
				e.printStackTrace();
			}
			if(error){
				System.out.println("Error exist!");
				continue;
			}
			
			pout.print(record);
			
			if(single)
				singleCount++;
			
			UpdateWorkloadModel(routing, predefined, txni);
			boolean printed = false;
			long CurrentMilis = System.currentTimeMillis();
			if(CurrentMilis - lastmemo >= thresh_memo){
				double tpmC = 6000000.0 * (double)(transactionCount - lastCount) / (double)(CurrentMilis - lastmemo) / 100.0;
				thro.print(String.format("%.2e", tpmC) + "\t" + String.valueOf(transactionCount) + "\t" + String.valueOf(transactionCount - lastCount) + "\t" + String.valueOf(CurrentMilis - lastmemo));
				lastmemo = CurrentMilis;
				lastCount = transactionCount;
				System.out.println("Processed: " + transactionCount + "/" + repeat * history.size());
				
				thro.print("\t" + String.valueOf(singleCount - lastSingle));
				lastSingle = singleCount;
				
				if(CurrentMilis - lastsearch >= thresh_rep){
					long beforeSearch = System.currentTimeMillis();
					cDesign = ADP.model.getDesignCost();
					double design = RepartitionSearch(transactionCount);
					System.out.println("Searched Plan: " + design + "\t" + "Current Plan:" + cDesign);
					long afterSearch = System.currentTimeMillis();
					int repCount = 0;
					if(1 - design/cDesign >= thresh_cost){
						try {
							repCount = RepartitionApply();
						} catch (SQLException e) {
							e.printStackTrace();
							e.getNextException().printStackTrace();
						}
						cDesign = rs.last_cost;
						rs.reset_design();
					}
					long afterapply = System.currentTimeMillis();
					lastsearch = afterapply;
					lastmemo = afterapply;
					
					thro.print("\t" + String.valueOf(afterSearch - beforeSearch)
							+ "\t" + String.valueOf(afterapply - afterSearch)
							+ "\t" + String.valueOf(repCount)
							+ "\t" + String.format("%.2e", adp.model.getDistCost())
							+ "\t" + String.format("%.2e", adp.model.getSkew()));
					
				}// Search for new design and check if it is time to do a repartitioning
				else{//Print some space position holder
					thro.print("\t \t \t \t \t ");
				}
				
				
				
				printed = true;
			}//Store the throughput
			
			if(printed){
				thro.print("\n");
			}
		}
			
		}
		thro.close();
		performance_out.close();
	}

	private static Map<String, PropertySet<String>> QueryRouting(
			Map<String, PropertySet<String>> tuples) {
		Map<String, PropertySet<String>> result = new HashMap<String, PropertySet<String>>();
		//String txni for debugging
		if(tuples == null ||tuples.isEmpty()){
			System.out.println("Empty Transaction!!");
			return result;
		}
		
		//Choose the best plan, and store it in the workload model
		//1. Planning
		Set<String> partition = new HashSet<String>();
		Set<Integer> queries = new HashSet<Integer>();
		Map<String, Set<Integer>> cover = new HashMap<String,Set<Integer>>();
		int i = 0; //Act as the query index,start from 1 to the size of quries
		//Max Coverage problem
		//Greedy Algorithm
		//1. Collect each partition's coverage
		//2. Greedily choose the partition cover the most queries until all the queries are covered

		for(Entry<String,PropertySet<String>> e:tuples.entrySet()){
			i++;
			boolean write = e.getValue().isWrite();
			for(String par:e.getValue()){
				
				if(cover.containsKey(par)){
					cover.get(par).add(i);
				}else{
					Set<Integer> qset = new HashSet<Integer>();
					qset.add(i);
					cover.put(par, qset);
				}
				if(write)
					i++;
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
		//Then we get the partition selector list, applied it to the tuples
		for(Entry<String,PropertySet<String>> e:tuples.entrySet()){
			if(e.getValue().isWrite()){
				result.put(e.getKey(), e.getValue());
				continue;
			}else{
				e.getValue().retainAll(partition);
				if(e.getValue().size() > 1){
					String sel = e.getValue().iterator().next();
					boolean write = e.getValue().isWrite();
					e.getValue().clear();
					e.getValue().add(sel);
					e.getValue().setWrite(write);
				}
				result.put(e.getKey(), e.getValue());
			}
		}
				
		return result;
		
	}
	
	private static void UpdateWorkloadModel(
			Map<String, PropertySet<String>> tuples,
			boolean predefined, String txni) {
		//String txni for debugging
		if(tuples == null ||tuples.isEmpty()){
			System.out.println("Empty Transaction!!");
			return;
		}
		
		if(predefined){
			//Use the same plan for already defined workloads
			ADP.model.insertRecord(tuples, txni);
			return;
		}
		
		//Choose the best plan, and store it in the workload model
		//1. Planning
		Set<String> partition = new HashSet<String>();
		Set<Integer> queries = new HashSet<Integer>();
		Map<String, Set<Integer>> cover = new HashMap<String,Set<Integer>>();
		int i = 0; //Act as the query index,start from 1 to the size of quries
		//Max Coverage problem
		//Greedy Algorithm
		//1. Collect each partition's coverage
		//2. Greedily choose the partition cover the most queries until all the queries are covered

		for(Entry<String,PropertySet<String>> e:tuples.entrySet()){
			i++;
			boolean write = e.getValue().isWrite();
			for(String par:e.getValue()){
				
				if(cover.containsKey(par)){
					cover.get(par).add(i);
				}else{
					Set<Integer> qset = new HashSet<Integer>();
					qset.add(i);
					cover.put(par, qset);
				}
				if(write)
					i++;
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
		//Then we get the partition selector list, applied it to the tuples
		for(Entry<String,PropertySet<String>> e:tuples.entrySet()){
			if(e.getValue().isWrite())
				continue;
			else{
				e.getValue().retainAll(partition);
				if(e.getValue().size() > 1){
					String sel = e.getValue().iterator().next();
					boolean write = e.getValue().isWrite();
					e.getValue().clear();
					e.getValue().add(sel);
					e.getValue().setWrite(write);
				}
			}
		}
				
		//2. Updating the model
		
		ADP.model.insertRecord(tuples,txni);
		
	}

	private static int RepartitionApply() throws SQLException {
		if(rs.hasNew){
			RepartitionApplier ra = new RepartitionApplier(adp);
			int i = rs.change.size();
			rs.hasNew = false;
			ra.Apply(rs.change, rs.last_design, rs.last_load, rs.last_cost);
			rs.change.clear();	
			return i;
		}
		return 0;
	}
	
	private static Vector<String> RepartitionQueries(){
		Vector<String> queries = null;
		if(rs.hasNew){
			RepartitionApplier ra = new RepartitionApplier(adp);
			int i = rs.change.size();
			rs.hasNew = false;
			queries = ra.getQuery(rs.change);
			rs.change.clear();	
		}
		return queries;
	}

	private static double RepartitionSearch(int tranCount) {
		//System.out.println("Before search load: ");
		//for(int i=1; i <= adp.getNodecount();i++){
		//	System.out.println(adp.model.getLoad(String.valueOf(i)));
		//}
		
		rs.NewSearch(tranCount);
		
		//System.out.println("After search load: ");
		//for(int i=1; i <= adp.getNodecount();i++){
		//	System.out.println(adp.model.getLoad(String.valueOf(i)));
		//}
		return rs.last_cost;
	}


	private static PropertySet<String> LookUpQuery(String key, boolean write, int tranCount) throws SQLException {
		PropertySet<String> result = new PropertySet<String>(write);
		Set<String> r = ADP.lookupTable.get(key,tranCount);
		if(r == null){
			result.addAll(adp.model.getInsertNodes());
		}else
			result.addAll(r);
		return result;
	}

	private static int initJTA(int thread_count, boolean exec) throws IOException, SQLException{
		int start = 0;
		int par = 5;
		Vector<Properties> inis = JTA.genBTM(par);
		 adp = new ADP(inis,"FileUndefined",5000,1000);
		//End of todo 1
		

        System.out.println("Start Load lookupTable");
        File lookup_addr = new File(System.getProperty("user.home") + "/Documents/Result/","lookuptable.ec2.jta.dump");
        ADP.lookupTable.loadLookupTable(lookup_addr.toString());
        System.out.println("End Load lookupTable");
        //End of todo 2
		
        System.out.println("Start Load Workload File");
        String filename = workload_file;
        FileReader infile = null;
        BufferedReader bin = null;
        String line = null;
        if(workload_file != null){
        
	        try {
	        	File f = new File(adp.workdir + filename);
	        		
	        	
				infile = new FileReader(f);
				bin = new BufferedReader(infile);
			} catch (FileNotFoundException e) {	
				e.printStackTrace();
			}
	        
	        LinkedList<String> workload = new LinkedList<String>();
			while((line = bin.readLine())!=null){
				//if(line.contains("INSERT"))
				//	continue;
				workload.addLast(line);
				
			}
			
			
			
			bin.close();
			infile.close();
		
			System.out.println("Workload Loaded");
			
			history = new LinkedList<String>();
			txnDef = new HashMap<String, LinkedList<String>>();
			
			processWorkloadJTA(workload, exec);//For repartition simulation
			if(exec)
				e = new Executioner[thread_count];
			
			System.out.println("Transaction Loaded: count = " + history.size());
			workload.clear();
		//End of todo 3
		System.out.println(" Workload Processed");
        }
		
		if(workload_file_par != null){
			try {
	        	File f = new File(adp.workdir + workload_file_par);
	        		
	        	
				infile = new FileReader(f);
				bin = new BufferedReader(infile);
			} catch (FileNotFoundException e) {	
				e.printStackTrace();
			}
			
			replist = new LinkedList<String>();
			txnPar = new HashMap<String,Vector<String>>();
			while((line = bin.readLine())!=null){
				String elem[] = line.split("\t");
				if(elem.length != 4){
					System.out.println("Error Line: " + line);
					continue;
				}
				String pa = elem[0];
				String key = elem[1];
				String gain = elem[2];
				String sql = elem[3];
				if(txnPar.containsKey(pa)){
					txnPar.get(pa).add(key + "\t" + sql);
				}else{
					Vector<String> s = new Vector<String>();
					s.add(key + "\t" + sql);
					txnPar.put(pa, s);
				}
				replist.addLast(line);
			}
			bin.close();
			infile.close();
		}
		
		if(poissdst != null){
			try {
	        	File f = new File(adp.workdir + poissdst);
	        		
	        	
				infile = new FileReader(f);
				bin = new BufferedReader(infile);
			} catch (FileNotFoundException e) {	
				e.printStackTrace();
			}
			
			load_dist = new LinkedList<Integer>();
			
			while((line = bin.readLine())!=null){
				load_dist.addLast(Integer.valueOf(line));
			}
			bin.close();
			infile.close();
		}
		
		
		try {
        	File f = new File(adp.workdir + filename + ".proc");
        	
        	if(!f.exists())
        		f.createNewFile();
        	
			out = new FileOutputStream((f), false);
			pout = new PrintStream(out);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Initial the output file
		// Processed workload will be stored to this file in the simulation step
		
		//RepartitionSearcher initialize
		rs = new RepartitionSearcher(adp);
		
		//Add some workload as initial history
		//start = initial_history(50000);
		
		System.out.println("Initialized!");
		return start;
	}
	
	private static int initTest(int thread_count, int thread_rep, boolean exec) throws IOException, SQLException{
		int start = 0;
		int par = 5;
		Vector<Properties> inis = JTA.genBTMTest(par);
		 adp = new ADP(inis,"test",5000,1000);
		//End of todo 1
		

        System.out.println("Start Load lookupTable");
        File lookup_addr = new File(System.getProperty("user.home") + "/Documents/Result/","lookuptable.ec2.jta.test1.dump");
        ADP.lookupTable.loadLookupTable(lookup_addr.toString());
        System.out.println("End Load lookupTable");
        //End of todo 2
		
        System.out.println("Start Load Workload File");
        String filename = workload_file;
        FileReader infile = null;
        BufferedReader bin = null;
        String line = null;
        if(workload_file != null){
        
	        try {
	        	File f = new File(adp.workdir + filename);
	        		
	        	
				infile = new FileReader(f);
				bin = new BufferedReader(infile);
			} catch (FileNotFoundException e) {	
				e.printStackTrace();
			}
	        history = new LinkedList<String>();
//	        LinkedList<String> workload = new LinkedList<String>();
			while((line = bin.readLine())!=null){
				//if(line.contains("INSERT"))
				//	continue;
//				workload.addLast(line);
				history.add(line);
				
			}
			
			
			
			
			
			bin.close();
			infile.close();
		
			System.out.println("Workload Loaded");
			
			
			//txnDef = new HashMap<String, LinkedList<String>>();
			
//			processWorkloadTest(workload, exec);//For repartition simulation
			if(exec){
				reppro = new Repartitioner[thread_rep];
				e = new Executioner[thread_count];
			}
			
			System.out.println("Transaction Loaded: count = " + history.size());
//			workload.clear();
		//End of todo 3
		System.out.println(" Workload Processed");
        }
		
		if(workload_file_par != null){
			try {
	        	File f = new File(adp.workdir + workload_file_par);
	        		
	        	
				infile = new FileReader(f);
				bin = new BufferedReader(infile);
			} catch (FileNotFoundException e) {	
				e.printStackTrace();
			}
			
			replist = new LinkedList<String>();
//			txnPar = new HashMap<String,Vector<String>>();
			while((line = bin.readLine())!=null){
//				String elem[] = line.split("\t");
//				if(elem.length != 4){
//					System.out.println("Error Line: " + line);
//					continue;
//				}
//				String pa = elem[0];
//				String key = elem[1];
//				String gain = elem[2];
//				String sql = elem[3];
//				if(txnPar.containsKey(pa)){
//					txnPar.get(pa).add(key + "\t" + sql);
//				}else{
//					Vector<String> s = new Vector<String>();
//					s.add(key + "\t" + sql);
//					txnPar.put(pa, s);
//				}
				replist.addLast(line);
			}
			
			Collections.shuffle(replist);
			
			bin.close();
			infile.close();
		}
		
		if(poissdst != null){
			try {
	        	File f = new File(adp.workdir + poissdst);
	        		
	        	
				infile = new FileReader(f);
				bin = new BufferedReader(infile);
			} catch (FileNotFoundException e) {	
				e.printStackTrace();
			}
			
			load_dist = new LinkedList<Integer>();
			
			while((line = bin.readLine())!=null){
				load_dist.addLast(Integer.valueOf(line));
			}
			bin.close();
			infile.close();
		}
		
		System.out.println("Initialized!");
		return start;
	}
	
	
	
	private static int initXC(int thread_count, boolean exec) throws IOException, SQLException{
		int start = 0;
		Properties ini = new Properties();
		ini.put("driver", "org.postgresql.Driver");
		ini.put("conn", "jdbc:postgresql://54.217.220.194:20011/test_2");
		ini.put("user", "postgres");
		ini.put("password", "sdh");
		adp = new ADP(ini,4,"FileUndefined",500,300);//ADP(Property, PartitionCount, KeylistFile, WindowSize, TopkSize)
		//End of todo 1
		

        System.out.println("Start Load lookupTable");
        File lookup_addr = new File(System.getProperty("user.home") + "/Documents/Result/","lookuptable.ec2.small.dump");
        ADP.lookupTable.loadLookupTable(lookup_addr.toString());
        System.out.println("End Load lookupTable");
        //End of todo 2
		
        System.out.println("Start Load Workload File");
        String filename = "P50NO50_4Warehouse_10min";
        FileReader infile = null;
        BufferedReader bin = null;
        
        try {
        	File f = new File(adp.workdir + filename);
        		
        	
			infile = new FileReader(f);
			bin = new BufferedReader(infile);
		} catch (FileNotFoundException e) {	
			e.printStackTrace();
		}
        
        LinkedList<String> workload = new LinkedList<String>();
		String line = null;
		while((line = bin.readLine())!=null){
			if(line.contains("INSERT"))
				continue;
			workload.addLast(line);
			
		}
		
		
		
		bin.close();
		infile.close();
		
		System.out.println("Workload Loaded");
		
		history = new LinkedList<String>();
		txnDef = new HashMap<String, LinkedList<String>>();
		
		if(!exec)
			processWorkloadXC(workload, false);//For repartition simulation
		else{
			processWorkloadXC(workload, true);//For Execution
			e = new Executioner[thread_count];
		}
		
		System.out.println("Transaction Loaded: count = " + history.size());
		workload.clear();
		//End of todo 3
		System.out.println("Workload Processed");
		
		try {
        	File f = new File(adp.workdir + filename + ".proc");
        	
        	if(!f.exists())
        		f.createNewFile();
        	
			out = new FileOutputStream((f), false);
			pout = new PrintStream(out);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Initial the output file
		// Processed workload will be stored to this file in the simulation step
		
		//RepartitionSearcher initialize
		rs = new RepartitionSearcher(adp);
		
		//Add some workload as initial history
		//start = initial_history(50000);
		
		System.out.println("Initialized!");
		return start;
	}
	
	private static void testAvgQueries(){
		int tot = 0;
		for(Entry<String, LinkedList<String>> e: txnDef.entrySet()){
			tot += e.getValue().size();
		}		System.out.println("Average Queries of Each Txn = " + (double)tot / (double)txnDef.size());
	}
	
	private static void processWorkloadXC(LinkedList<String> workload, boolean partitioned) {
		String flag = null;
		//Set<String> pars = new HashSet<String>();
		Set<String> tup = new HashSet<String>();
		LinkedList<String> queries = new LinkedList<String>();
		boolean error = false;
		for(Iterator<String> it = workload.listIterator();it.hasNext();){
			String query = it.next();
			if(flag == null || flag.equalsIgnoreCase("COMMIT") || query.toLowerCase().contains("begin")){
				flag = query;
				error = false;
				continue;
			}else if(query.toLowerCase().contains("commit")){
				flag = "COMMIT";
				if(!error){
					String key = ADP.lookupTable.getTxnKey(tup);
					//adp.lookupTable.insertTxn(key, pars);
					
					history.addLast(key);
//					if(System.currentTimeMillis() % 5 == 1)
//						for(int i=0; i < (int)Math.ceil(Math.random() * 100); i++)
//							history.addLast(key);
					txnDef.put(key, queries);
					
					tup.clear();
					//pars = new HashSet<String>();
					queries = new LinkedList<String>();
				}
			}else{
				if(error)
					continue;
				String[] elem = query.split("\t");
        		if(elem.length != 3){
        			System.out.println("Error: " + query);
        			error = true;
        			continue;
        		}else{
        			String key = elem[0];
        			String par = elem[1];
        			String sql = elem[2];//The partition is not defined now, with '?' in query
        			
        			if(partitioned)
        				sql = sql.replace("?", par);
        			
        			
        			//if(sql.contains("INSERT"))
        			//	continue;
        			
        			if(key!="null"){
        				if(!tup.contains(key)){
        					tup.add(key);
        				}else{
        					continue;
        				}
        				//pars.add(par);
        			}
        			String keySql = key + "\t" + sql;
        			queries.addLast(keySql);
        		}
			}
		}
		
		//Collections.shuffle(history);// Comments this line when directly execute the workload
		
	}

	private static void processWorkloadJTA(LinkedList<String> workload, boolean partitioned) {
		String flag = null;
		//Set<String> pars = new HashSet<String>();
		Set<String> tup = new HashSet<String>();
		LinkedList<String> queries = new LinkedList<String>();
		boolean error = false;
		for(Iterator<String> it = workload.listIterator();it.hasNext();){
			String query = it.next();
			if(flag == null || flag.equalsIgnoreCase("COMMIT") || query.toLowerCase().contains("begin")){
				flag = query;
				error = false;
				continue;
			}else if(query.toLowerCase().contains("commit")){
				flag = "COMMIT";
				if(!error){
					String key = ADP.lookupTable.getTxnKey(tup);
					//adp.lookupTable.insertTxn(key, pars);
					
					history.addLast(key);
//					if(System.currentTimeMillis() % 5 == 1)
//						for(int i=0; i < (int)Math.ceil(Math.random() * 100); i++)
//							history.addLast(key);
					txnDef.put(key, queries);
					//txnPar.put(key, tup);
					
					tup.clear();
					//pars = new HashSet<String>();
					queries = new LinkedList<String>();
				}
			}else{
				if(error)
					continue;
				String[] elem = query.split("\t");
        		if(elem.length != 3 && elem.length != 2){
        			System.out.println("Error: " + query);
        			error = true;
        			continue;
        		}else{
        			String sql = null,par = null,key = null;
        			if(elem.length == 3){
	        			key = elem[0];
	        			par = elem[1];
	        			sql = elem[2];//The partition is not defined now, with '?' in query
        			}else{
        				par = elem[0];
        				sql = elem[1];
        			}
        			
        			
        			
        			//if(sql.contains("INSERT"))
        			//	continue;
        			
        			if(key==null || !key.equals("null")){
        				if(!tup.contains(key)){
        					tup.add(key);
        				}else{
        					//continue;
        				}
//        				pars.add(par);
        			}
        			queries.addLast(key + "\t" + par + "\t" + sql);
        			
//        			if(partitioned){
//        				String parSql = par + "\t" + sql;
//        				queries.addLast(parSql);
//        			}else{
//	        			String keySql = key + "\t" + sql;
//	        			queries.addLast(keySql);
//        			}
        		}
			}
		}
		
		//Collections.shuffle(history);// Comments this line when directly execute the workload
		
	}
	
	private static void processWorkloadTest(LinkedList<String> workload, boolean partitioned) {
		String flag = null;
		//Set<String> pars = new HashSet<String>();
		Set<String> tup = new HashSet<String>();
		LinkedList<String> queries = new LinkedList<String>();
		boolean error = false;
		for(Iterator<String> it = workload.listIterator();it.hasNext();){
			String query = it.next();
			if(flag == null || flag.equalsIgnoreCase("COMMIT") || query.toLowerCase().contains("begin")){
				flag = query;
				error = false;
				continue;
			}else if(query.toLowerCase().contains("commit")){
				flag = "COMMIT";
				if(!error){
					String key = getTestKey(tup);
					
					history.addLast(key);

					txnDef.put(key, queries);
					
					tup.clear();
					queries = new LinkedList<String>();
				}
			}else{
				if(error)
					continue;
				String[] elem = query.split("\t");
        		if(elem.length != 3){
        			System.out.println("Error: " + query);
        			error = true;
        			continue;
        		}else{
        			String sql = null,par = null,key = null;
        			key = elem[0];
        			par = elem[1];
        			sql = elem[2];
        			
        			if(key==null || !key.equals("null")){
        				if(!tup.contains(key)){
        					tup.add(key);
        				}
        			}
        			queries.addLast(key + "\t" + par + "\t" + sql);

        		}
			}
		}

	}

	private static String getTestKey(Set<String> tup) {
		int max = 0;
		for(String s:tup){
			int n = Integer.valueOf(s);
			if(n>max)
				max = n;
		}
		return String.valueOf(max);
	}

	private static int initial_history(int i) throws SQLException {
		// TODO Add i transactions as initial training data
		//		1. Add all the i transactions
		//		2. Repartitioning (Didn't do this currently)
		//		3. Update LookupTable and Workload Model
		
		int count = 0;
		
		//boolean error = false;
		double mul = (double)i / (double)history.size();
		
		System.out.println("Mul = " + Math.ceil(mul));
		
		for(int j=0;j<Math.ceil(mul);j++)
			for(String txni:history){
				boolean predefined = false;
				
				if(count >= i)
					break;
				if(count % 5000 == 1){
					System.out.println(count + "/" + i);
				}
				count++;
				//boolean predefined = false;
				Map<String, PropertySet<String>> routing = ADP.model.getRouting(txni);
				if(routing != null){
					UpdateWorkloadModel(routing,true, txni);
					continue;
				}else
					routing = new HashMap<String,PropertySet<String>>();
				for(Iterator<String> it = txnDef.get(txni).listIterator(); it.hasNext();){
					String query = it.next();
					String[] elem = query.split("\t");
					String key = elem[0];
					query = elem[1];
        			//if(query.contains(ADP.partition_col)){     
					String table;
					Map<String,String> columns = new HashMap<String,String>();
					QueryProcessor qp = new QueryProcessor(query);
					List<QueryPlan> plans = qp.processQuery(count);
					PropertySet<String> partition;

					boolean write = false;
					if(query.toLowerCase().contains("update"))
						write = true;

					if(plans.isEmpty()){
						//error = true;
						System.out.println("Error processing query: " + query);
						break; // Error in transaction
					}

					//key = plans.iterator().next().getKey();
					columns = plans.iterator().next().getColumns();
					table = plans.iterator().next().getTableName();

//					if(columns.containsKey(ADP.partition_col))
//						columns.remove(ADP.partition_col);
					if(ADP.lookupTable.getRaw(key) == null)
						ADP.lookupTable.updateRaw(key, columns, table);

					partition = LookUpQuery(key, write, count);
					if(partition == null){
						//error = true;
						System.out.println("Cannot find target: " + query);
						break;
					}

					routing.put(key, partition);
        			//}	
				}
				UpdateWorkloadModel(routing,false,txni);
			}
		
		return count;
		
	}

	private static void store() throws IOException{
		
		pout.close();
		out.close();
		
		
		
		File lookup_addr = new File(adp.workdir,"lookuptable.jta.edit");
		lookup_addr.delete();
		System.out.println("LookupTable Save begin");
		if(!lookup_addr.exists())
			try {
				lookup_addr.createNewFile();
				ADP.lookupTable.saveLookuptable(lookup_addr.toString());
			} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("LookupTable Saved: " + lookup_addr);
	}
	
	public static void testXA() throws NotSupportedException, SystemException, SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException{
		ds = new PoolingDataSource();
		ds.setClassName("org.postgresql.xa.PGXADataSource");
		ds.setUniqueName("pgsql");
		ds.setMaxPoolSize(5);
		ds.getDriverProperties().setProperty("databaseName", "test_1");
		ds.getDriverProperties().setProperty("user", "postgres");
		ds.getDriverProperties().setProperty("password", "sdh");
		ds.getDriverProperties().setProperty("serverName", "54.217.220.194");
		ds.getDriverProperties().setProperty("portNumber", "20011");
		ds.setAllowLocalTransactions(true);
		ds.init();
		
//		String sql1 = "UPDATE warehouse SET w_ytd=w_ytd + '1380.92' WHERE w_id = '13' AND Partition = ?";
//		String sql2 = "UPDATE district SET d_ytd=d_ytd + '1380.92' WHERE d_w_id = '13' AND Partition = ? AND d_id = '7'";
//		String sql3 = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = '13' AND c_d_id = '7' AND Partition = ? AND c_id = '1765'";
		
		
		btm = TransactionManagerServices.getTransactionManager();
		
//		btm.begin();
//		try{
//			Connection c = ds.getConnection();
//			
//			PreparedStatement st = c.prepareStatement(sql1);
//			st.setInt(1, 3);
//			st.executeUpdate();
//			c.close();
//			btm.commit();
//			
//		}catch(SQLException ex){
//			ex.printStackTrace();
//			btm.rollback();
//		}
		
		
	}
	
	
	public static void StaticPartitionTest1(){
		try {
			initTest(0, 0,false);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		File f = new File(adp.workdir + "RepartitionQuery");
		if(f.exists()){
			f.delete();
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		FileOutputStream performance_out;
		try {
			performance_out = new FileOutputStream(f);
			PrintStream thro = new PrintStream(performance_out);
			
			for(String txn:history){
				int par = (Integer.valueOf(txn) % 25 + 1) / 5;
				thro.println("BEGIN");
				for(String line:txnDef.get(txn)){
					String elem[] = line.split("\t");
					if(elem.length != 3){
						System.out.println("Error line: " + line);
						continue;
					}
					String key = elem[0];
					String opar = elem[1];
					String sql = elem[2];
					
					if(opar.equalsIgnoreCase(String.valueOf(par)))
						continue;
					else{
						String rep = "INSERT INTO load " +
							       " (id, data) " +
							       "VALUES (" + key + "," +
							       key + ")";
						thro.println(par + "\t" + key + "\t" + rep);
					}
				}
				thro.println("COMMIT");
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void StaticPartition(){
		try {
			initJTA(5, false);
			initial_history(history.size() * 5);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		File f = new File(adp.workdir + "RepartitionQuery");
		if(f.exists()){
			f.delete();
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		FileOutputStream performance_out;
		try {
			performance_out = new FileOutputStream(f);
			PrintStream thro = new PrintStream(performance_out);
			double cDesign = ADP.model.getDesignCost();
			double design = RepartitionSearch(history.size());
			System.out.println("Searched Plan: " + design + "\t" + "Current Plan:" + cDesign);
			System.out.println("Sngle Transaction: " + rs.getSingle());
			int repCount = 0;
			Vector<String> queries = RepartitionQueries();
			if(queries != null){
				for(String q:queries){
					thro.println(q);
				}
				thro.close();
				performance_out.close();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		

	}
	
	private static void RepApply(int capacity) throws IOException, InterruptedException, NotSupportedException, SystemException, SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException{
		//workload_file = "10.0t1@137959729";
		workload_file_par = "RepartitionQueryS398-1157";
//		poissdst = "PoissonRND";
		
//		int THROUGHPUT_LIMIT = 200;//Limit: 200 txns per minutes
		
		try {
			initJTA(5,true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		int i = 0;
		int intval = 0;//Dynamic throughput limit
		int avg = 0;
		int maxload = Collections.max(load_dist);
		for(int d:load_dist)
			avg += d;
		avg = avg / load_dist.size();
		
		int cap = (int)(avg / (float)maxload * capacity);
//		System.out.println("AVG: " + avg + "/" + maxload);
		long start = System.currentTimeMillis();
		long step = start;
		//1:Stop normal transaction and execute as much repartition query as possible
		
		System.out.println("Start(" + replist.size() + ")");
		int last_gain = 0;
		for(String load:replist){
			//2: Apply the same amount of queries to the system each time interval
//			int cap = (int) ( capacity * load_dist.get(intval) / (float)maxload);
			
			long now = System.currentTimeMillis();
			if(i>cap || now - step > 600000){
				i=0;
				System.out.println(60000 - now + step);
				Thread.sleep(60000 - now + step);
				step = System.currentTimeMillis();
				intval++;
			}
			
			String[] elem = load.split("\t");
			String par = elem[0];
			String key = elem[1];
			String gain = elem[2];
			String sql = elem[3];
			adp.jta.begin();
			try {
				adp.newQueryJTA(sql, par, ADP.QueryType.INSERT);
			} catch (SQLException e) {
				System.err.println("Failed to execute: " + sql);
				adp.jta.abort();
				e.printStackTrace();
				continue;
			}
			adp.jta.commit();
			i++;
		}
		long end = System.currentTimeMillis();
		System.out.println("Time consumption: " + String.valueOf((end - start)/1000) + " seconds");
		
	}
	
	private static void RepCombine() throws IOException, InterruptedException, NotSupportedException, SystemException, SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException{
		workload_file = "10.0t1S398@138052813";
		workload_file_par = "RepartitionQuery";
		poissdst = "PoissonRND";
		
		int THROUGHPUT_LIMIT = 200;//Limit: 200 txns per minutes
		
		
		try {
			initJTA(5,true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		int avg = 0;
		int maxload = Collections.max(load_dist);
		Map<String, Iterator<String>> repq = new HashMap<String, Iterator<String>>();
		for(Entry<String, Vector<String>> e:txnPar.entrySet()){
			repq.put(e.getKey(), e.getValue().iterator());
		}
		Iterator<String> it = history.iterator();
		
		for(int d:load_dist)
			avg += d;
		avg = avg / load_dist.size();
		
		//int cap = (int)(avg / (float)maxload * capacity);
		System.out.println("AVG: " + avg + "/" + maxload);
		System.out.println("Workload Size: " + history.size());

		File f = new File(adp.workdir + "CombinedQuery1");
		if(f.exists()){
			f.delete();
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		FileOutputStream result_out;
		
		result_out = new FileOutputStream(f);
		PrintStream combine = new PrintStream(result_out);
			
		System.out.println("Start(" + replist.size() + ")");
		int last_gain = 0;
		int length = 45;
		int pass = 0;
		
		for(int intval:load_dist){
			//2: Apply the same amount of queries to the system each time interval
			int cap = (int) ( THROUGHPUT_LIMIT * intval / (float)maxload);
			System.out.print("Capacity: " + cap + "\t");
			Map<String, Integer> load = new HashMap<String, Integer>();
			int maxpar = 0;
			int txnc = 0;
			
			while(maxpar < cap){//TODO: Change the loop condition here, cap limits the maximum connection to one datanode
				txnc++;
				if(!it.hasNext()){
					it = history.iterator();
				}
				combine.println("BEGIN");
				String txn = it.next();
				HashMap<String, String> queries = new HashMap<String, String>();
				Map<String, PropertySet<String>> tuples = new HashMap<String, PropertySet<String>>();
				Set<String> partition = adp.model.getPartitions(txn);
				for(String sql:txnDef.get(txn)){
					String elem[] = sql.split("\t");
					String key = elem[0];
					String par = elem[1];
					String query = elem[2];
					PropertySet<String> tuple_par = new PropertySet<String>();
					tuple_par.addAll(adp.lookupTable.get(key));
					if(tuple_par.isEmpty()){
						adp.lookupTable.insert(key, par);
						tuple_par = new PropertySet<String>();
						tuple_par.add(par);
					}
					if(query.contains("select"))
						tuple_par.setWrite(false);
					else
						tuple_par.setWrite(true);
					tuples.put(key, tuple_par);
				}
				tuples = QueryRouting(tuples);
				
				for(String sql:txnDef.get(txn)){
					String elem[] = sql.split("\t");
					String key = elem[0];
					//String par = elem[1];
					String query = elem[2];
					PropertySet<String> pars = tuples.get(key);
					for(String par:pars){
						if(load.containsKey(par)){
							load.put(par, load.get(par) + 1);
						}else{
							load.put(par, 1);
						}
						if(maxpar < load.get(par))
							maxpar = load.get(par);
						combine.println(key + "\t" + par + "\t" + query);
					}
					
				}
				combine.println("COMMIT");
				
			}
			
			System.out.print("TxnAdded: " + txnc + "\t");
			
			pass++;
			if(pass < 15){
				continue;
			}
			
			
			Map<String, Integer> resources = new HashMap<String, Integer>();
			int min_res = THROUGHPUT_LIMIT, max_res = 0;
			for(Entry<String, Integer> e: load.entrySet()){
				resources.put(e.getKey(), THROUGHPUT_LIMIT - e.getValue());
				if(min_res > resources.get(e.getKey())){
					min_res = resources.get(e.getKey());
				}
				if(max_res < resources.get(e.getKey())){
					max_res = resources.get(e.getKey());
				}
			}
			txnc = 0;
			if(max_res <= 0)
				continue;
			//Solution 1: Apply constant number of repartition queries
			
			//Solution 2: Apply as much as possible, each query one transaction
			for(Entry<String, Integer> e: resources.entrySet()){
				for(int i=0;i<e.getValue();i++){
					String repsql = null;
					if(repq.get(e.getKey()).hasNext()){
						repsql = repq.get(e.getKey()).next();
					}else{
						continue;
					}
					txnc++;
					String elem[] = repsql.split("\t");
					if(elem.length != 2){
						System.out.println("Error Line: " + repsql);
						continue;
					}
					String key = elem[0];
					String sql = elem[1];
					adp.lookupTable.insert(key, e.getKey());
					combine.println("BEGIN");
					combine.println(key + "\t" + e.getKey() + "\t" + sql);
					combine.println("COMMIT");
				}
				
			}
			
			System.out.print("RepTxn: " + txnc);
			System.out.println();
			
			if(pass > length)
				break;
			
		}
	}
	
}