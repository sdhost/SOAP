package dk.sdu.ADP.debug;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;

import pojo.Load;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.ADP.QueryType;
import dk.sdu.ADP.JTA.JTA;

public class Executioner implements Runnable {
	
	LinkedList<String> history;
	Map<String,LinkedList<String>> txnDef;
	Map<String, Integer> txnFreq;
	Map<String,Boolean> txnSingle;
	Iterator<Map.Entry<String, Boolean>> rep_pos;
	Integer freqThreshold;
	//LinkedList<Integer> loaddist;
	boolean init = false;
	boolean isxc = false;
	boolean istest = false;
	Statement st = null;
	String name;
	int count;
	long sum_latency = 0;
	long max_latency = 0;
	public int rep;
	public int repc;
	JTA jta;
	double reprate;
	int total_prog;
	int prog;
	double write_rate;
	Iterator<String> step;
	long interval;
	int tsize = 5;
	int rep_amount = 0;
	boolean isrep = false;
	
	boolean stop = false;
	private int amount; //Number of transactions that will be attached with repartition queries
	
	
	public Executioner(String name, LinkedList<String> history, Map<String,LinkedList<String>> txnDef, Connection conn) throws SQLException{
		this.history = history;
		this.txnDef = txnDef;
		this.name = name;
		this.count = 0;
		this.st = conn.createStatement();
		init = true;
		isxc = true;
	}
	
	public Executioner(String name, LinkedList<String> history, Map<String,LinkedList<String>> txnDef, JTA jta, int rep_count) throws SQLException{
		this.history = history;
		this.txnDef = txnDef;
		this.name = name;
		this.count = 0;
		this.jta = jta;
		this.rep = rep_count;
		this.repc = 0;
		init = true;
		isxc = false;
	}
	public Executioner(String name, LinkedList<String> history, Map<String,LinkedList<String>> txnDef, JTA jta, double rate){
		this.history = history;
		this.txnDef = txnDef;
		//this.loaddist = loaddist;
		this.name = name;
		this.count = 0;
		this.jta = jta;
		this.reprate = rate;
		init = true;
		isxc = false;
		istest = true;
		step = history.iterator();
		
	}
	
	public Executioner(String name, LinkedList<String> history, JTA jta, double wrate){
		this.name = name;
		this.history = history;
		this.write_rate = wrate;
		this.jta = jta;
		init = true;
		isxc = false;
		istest = true;
		step = history.iterator();
		this.count = 0;
		this.reprate = 0;
	}
	
	public Executioner(String name, LinkedList<String> history, JTA jta,
			double wrate, int tsize) {
		this.name = name;
		this.history = history;
		this.write_rate = wrate;
		this.jta = jta;
		init = true;
		isxc = false;
		istest = true;
		this.step = history.iterator();
		this.count = 0;
		this.reprate = 0;
		this.tsize = tsize;
		
	}
	
	public void setTsize(int tsize){
		this.tsize = tsize;
	}
	
	public void initTxnPiggyback(LinkedList<String> replist){
		this.prog = 0;
		this.total_prog = 0;
		this.txnFreq = new LinkedHashMap<String, Integer>();
		this.txnSingle = new HashMap<String, Boolean>();
		for(String line: replist){
			String[] elem = line.split("\t");
			String txn = elem[0];
			int freq = Integer.valueOf(elem[1]);
			this.txnFreq.put(txn, freq);
			this.txnSingle.put(txn, false);
			this.total_prog += freq;
		}
		this.rep_pos = this.txnSingle.entrySet().iterator();
		this.isrep = true;
	}
	
	public void setPiggybackAmount(int amount){
		this.amount = amount;
		Map.Entry<String, Boolean> e = null;
		for(int i=0;i<this.amount;i++)
			if(this.rep_pos.hasNext())
				e = this.rep_pos.next();
		if(e != null)
			this.freqThreshold = this.txnFreq.get(e.getKey());
		else
			this.freqThreshold = 0;
	}

	public void stopThread(){
		this.stop = true;
	}
	
	public boolean isStopped(){
		return this.stop;
	}
	
	public int getCount(){
		return this.count;
	}
	
	public long getSumLatency(){
		long result = this.sum_latency;
		this.sum_latency = 0;
		return result;
	}
	
	public long getMaxLatency(){
		long result = this.max_latency;
		this.max_latency = 0;
		return result;
	}
	
	
	public double getRepRate(){
		if(this.prog == this.total_prog)
			return 1.0d;
		return (double)(this.prog / (double) this.total_prog);
	}
	
	public void setRepRate(double progress){
		this.reprate = progress;
	}
	
	public long getInterval(){
		return this.interval;
	}
	public void setInterval(long deadlinet){
		this.interval = deadlinet;
	}
	
//	private void runRandom(){
//		this.stop = false;
//		long start = System.currentTimeMillis();
//		if(init && isxc && !istest){
//			for(String key:history){
//				
//				for(String sql:txnDef.get(key)){
//					String[] elem = sql.split("\t");
//					String rsql = elem[1];
//					ResultSet rs = null;
//					int rsc = 0;
//					if(sql.toUpperCase().contains("SELECT")){
//						try {
//							rs = st.executeQuery(rsql);
//							if(rs == null || !rs.next()){
//								System.out.println("Error for query: " + rsql);
//							}
//						} catch (SQLException e) {
//							System.err.println(rsql);
//							e.printStackTrace();
//						}
//					}
//					else{
//						try {
//							rsc = st.executeUpdate(rsql);
//							if(rsc == 0){
//								System.out.println("Error for query: " + rsql);
//							}
//						} catch (SQLException e) {
//							System.err.println(rsql);
//							e.printStackTrace();
//						}
//						
//					}
//				}
//			count++;
//			if(stop){
//				System.out.println("Thread " + name + " stopped, executed " + count + " transactions");
//				return;
//			}
//			}
//			System.out.println("Thread " + name + " stopped, executed " + count + " transactions");
//		}
//		else if(init && !isxc && !istest){
//			int step_count = count+repc;
//			for(String key:history){
//				long istart = System.currentTimeMillis();
//				while(true){
//					try {
//						jta.begin();
//						if(jta.getStatus() != Status.STATUS_NO_TRANSACTION)
//							break;
//						this.wait(1000);
//					} catch (NotSupportedException e1) {
//						e1.printStackTrace();
//					} catch (SystemException e1) {
//						e1.printStackTrace();
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
//				boolean rep_txn = false;
//				for(String sql:txnDef.get(key)){
//					if(sql.toUpperCase().contains("INSERT")){
//						rep_txn = true;
//						this.repc++;
//					}
//					String[] elem = sql.split("\t");
//					int par = Integer.valueOf(elem[1]);
//					String rsql = elem[2];
//					ResultSet rs = null;
//					int rsc = 0;
//					if(sql.toUpperCase().contains("SELECT")){
//						try {
//							jta.query(rsql, par, QueryType.SELECT);
//						} catch (SQLException e) {
//							System.err.println(rsql);
//							e.printStackTrace();
//							try {
//								jta.abort();
//							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
//							} catch (SecurityException e1) {
//								e1.printStackTrace();
//							} catch (SystemException e1) {
//								e1.printStackTrace();
//							}
//						} catch (InterruptedException e) {
//							try {
//								jta.abort();
//							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
//							} catch (SecurityException e1) {
//								e1.printStackTrace();
//							} catch (SystemException e1) {
//								e1.printStackTrace();
//							}
//							e.printStackTrace();
//						}
//					}
//					else if(sql.toUpperCase().contains("UPDATE")){
//						try {
//							jta.query(rsql, par, QueryType.UPDATE);
//						} catch (SQLException e) {
//							System.err.println(rsql);
//							e.printStackTrace();
//							try {
//								jta.abort();
//							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
//							} catch (SecurityException e1) {
//								e1.printStackTrace();
//							} catch (SystemException e1) {
//								e1.printStackTrace();
//							}
//						} catch (InterruptedException e) {
//							try {
//								jta.abort();
//							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
//							} catch (SecurityException e1) {
//								e1.printStackTrace();
//							} catch (SystemException e1) {
//								e1.printStackTrace();
//							}
//							e.printStackTrace();
//						}
//						
//					}else if(sql.toUpperCase().contains("INSERT")){
//						try {
//							jta.query(rsql, par, QueryType.INSERT);
//						} catch (SQLException e) {
//							System.err.println(rsql);
//							e.printStackTrace();
//							try {
//								jta.abort();
//							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
//							} catch (SecurityException e1) {
//								e1.printStackTrace();
//							} catch (SystemException e1) {
//								e1.printStackTrace();
//							}
//						} catch (InterruptedException e) {
//							try {
//								jta.abort();
//							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
//							} catch (SecurityException e1) {
//								e1.printStackTrace();
//							} catch (SystemException e1) {
//								e1.printStackTrace();
//							}
//							e.printStackTrace();
//						}
//					}else if(sql.toUpperCase().contains("DELETE")){
//						try {
//							jta.query(rsql, par, QueryType.DELETE);
//						} catch (SQLException e) {
//							System.err.println(rsql);
//							e.printStackTrace();
//							try {
//								jta.abort();
//							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
//							} catch (SecurityException e1) {
//								e1.printStackTrace();
//							} catch (SystemException e1) {
//								e1.printStackTrace();
//							}
//						} catch (InterruptedException e) {
//							try {
//								jta.abort();
//							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
//							} catch (SecurityException e1) {
//								e1.printStackTrace();
//							} catch (SystemException e1) {
//								e1.printStackTrace();
//							}
//							e.printStackTrace();
//						}
//					}
//				}
//			if(!rep_txn)
//				count++;
//			if(stop){
//				long end = System.currentTimeMillis();
//				double throughput = count * 60000.0 / (double)(end - start);
//				System.out.println("Thread " + name + " stopped, executed " + count + " transactions, Throughput is: " + throughput);
//				return;
//			}
//			try {
//				jta.commit();
//			} catch (SecurityException e) {
//				e.printStackTrace();
//			} catch (IllegalStateException e) {
//				e.printStackTrace();
//			} catch (RollbackException e) {
//				e.printStackTrace();
//			} catch (HeuristicMixedException e) {
//				e.printStackTrace();
//			} catch (HeuristicRollbackException e) {
//				e.printStackTrace();
//			} catch (SystemException e) {
//				e.printStackTrace();
//			}
//			long end = System.currentTimeMillis();
//			if(end - istart >= 20000 || count - step_count + repc >= 100){
//				step_count = count+repc;
//				try {
//					Thread.sleep(end - istart);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}
//			}
//			long end = System.currentTimeMillis();
//			double throughput = count * 20000.0 / (double)(end - start);
//			System.out.println("Thread " + name + " stopped, executed " + count + " transactions, Throughput is: " + throughput);
//			
//		}else if(init && !isxc && istest){
//			Random rnd = new Random();
//			rnd.setSeed(System.currentTimeMillis());
//			while(true){
//				long current = System.currentTimeMillis();
//				if(stop){
//					synchronized(this){
//						try {
//							this.wait();
//						} catch (InterruptedException e) {
//							e.printStackTrace();
//						}
//					}
//				}
//				String txn = null;
//				if(step.hasNext()){
//					txn = step.next();
//				}else{
//					step = history.iterator();
//					txn = step.next();
//				}
//				float f = rnd.nextFloat();
//				while(true){			
//					
//					try {
//						jta.begin();
//						if(jta.getStatus() != Status.STATUS_NO_TRANSACTION)
//							break;
//						this.wait(1000);
//					} catch (NotSupportedException e1) {
//						e1.printStackTrace();
//					} catch (SystemException e1) {
//						e1.printStackTrace();
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
//				if(f < reprate){//Current transaction will be single
//					
//					int par = Math.abs(rnd.nextInt()) % 5 +1;
//					int mul = Math.abs(rnd.nextInt()) % 100000;
//					int key = mul * 5 + par - 1;
//					for(String sql:txnDef.get(txn)){
//						String[] elem = sql.split("\t");
//						//String key = elem[0];
//						//int par = Integer.valueOf(elem[1]);
//						String rsql = elem[2];
//						if(rsql.toUpperCase().contains("SELECT")){
//							String fsql = "SELECT * from load WHERE id=" + String.valueOf(key);
//							try {
//								jta.query(fsql, par, QueryType.SELECT);
//							} catch (SQLException e) {
//								try {
//									jta.abort();
//								} catch (IllegalStateException e1) {
//									e1.printStackTrace();
//								} catch (SecurityException e1) {
//									e1.printStackTrace();
//								} catch (SystemException e1) {
//									e1.printStackTrace();
//								}
//								e.printStackTrace();
//							} catch (InterruptedException e) {
//								try {
//									jta.abort();
//								} catch (IllegalStateException e1) {
//									e1.printStackTrace();
//								} catch (SecurityException e1) {
//									e1.printStackTrace();
//								} catch (SystemException e1) {
//									e1.printStackTrace();
//								}
//								e.printStackTrace();
//							}
//						}else if(rsql.toUpperCase().contains("UPDATE")){
//							String fsql = "UPDATE load set data=" + String.valueOf(key) + " WHERE id=" + String.valueOf(key);
//							try {
//								jta.query(fsql, par, QueryType.UPDATE);
//							} catch (SQLException e) {
//								try {
//									jta.abort();
//								} catch (IllegalStateException e1) {
//									e1.printStackTrace();
//								} catch (SecurityException e1) {
//									e1.printStackTrace();
//								} catch (SystemException e1) {
//									e1.printStackTrace();
//								}
//								e.printStackTrace();
//							} catch (InterruptedException e) {
//								try {
//									jta.abort();
//								} catch (IllegalStateException e1) {
//									e1.printStackTrace();
//								} catch (SecurityException e1) {
//									e1.printStackTrace();
//								} catch (SystemException e1) {
//									e1.printStackTrace();
//								}
//								e.printStackTrace();
//							}
//						}
//					}
//				}else{
//					for(String sql:txnDef.get(txn)){
//						String elem[] = sql.split("\t");
//						String key = elem[0];
//						int par = Integer.valueOf(elem[1]);
//						String rsql = elem[2];
//						if(rsql.toUpperCase().contains("SELECT")){
//							String fsql = "SELECT * from load WHERE id=" + String.valueOf(key);
//							try {
//								jta.query(fsql, par, QueryType.SELECT);
//							} catch (SQLException e) {
//								try {
//									jta.abort();
//								} catch (IllegalStateException e1) {
//									e1.printStackTrace();
//								} catch (SecurityException e1) {
//									e1.printStackTrace();
//								} catch (SystemException e1) {
//									e1.printStackTrace();
//								}
//								e.printStackTrace();
//							} catch (InterruptedException e) {
//								try {
//									jta.abort();
//								} catch (IllegalStateException e1) {
//									e1.printStackTrace();
//								} catch (SecurityException e1) {
//									e1.printStackTrace();
//								} catch (SystemException e1) {
//									e1.printStackTrace();
//								}
//								e.printStackTrace();
//							}
//						}else if(rsql.toUpperCase().contains("UPDATE")){
//							String fsql = "UPDATE load set data=" + String.valueOf(key) + " WHERE id=" + String.valueOf(key);
//							try {
//								jta.query(fsql, par, QueryType.UPDATE);
//							} catch (SQLException e) {
//								try {
//									jta.abort();
//								} catch (IllegalStateException e1) {
//									e1.printStackTrace();
//								} catch (SecurityException e1) {
//									e1.printStackTrace();
//								} catch (SystemException e1) {
//									e1.printStackTrace();
//								}
//								e.printStackTrace();
//							} catch (InterruptedException e) {
//								try {
//									jta.abort();
//								} catch (IllegalStateException e1) {
//									e1.printStackTrace();
//								} catch (SecurityException e1) {
//									e1.printStackTrace();
//								} catch (SystemException e1) {
//									e1.printStackTrace();
//								}
//								e.printStackTrace();
//							}
//						}
//					}
//				}
//				
//				try {
//					jta.commit();
//				} catch (SecurityException e) {
//					e.printStackTrace();
//				} catch (IllegalStateException e) {
//					e.printStackTrace();
//				} catch (RollbackException e) {
//					e.printStackTrace();
//				} catch (HeuristicMixedException e) {
//					e.printStackTrace();
//				} catch (HeuristicRollbackException e) {
//					e.printStackTrace();
//				} catch (SystemException e) {
//					e.printStackTrace();
//				}
//				count++;
//			}
//		}
//	}
//	
	private void runGiven(){
		Random rnd = new Random();
		rnd.setSeed(System.currentTimeMillis());
		while(true){
			boolean error = false;
			if(stop){
				synchronized(this){
					try {
						this.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
						continue;
					}
				}
			}
			long current = System.currentTimeMillis();
			String txn = null;
			if(step.hasNext()){
				txn = step.next();
			}else{
				step = history.iterator();
				txn = step.next();
			}
			float f = rnd.nextFloat();
			float w = rnd.nextFloat();
			while(true){			
				
				try {
					jta.begin();
					if(jta.getStatus() != Status.STATUS_NO_TRANSACTION)
						break;
					//this.wait(1000);
				} catch (NotSupportedException e1) {
					e1.printStackTrace();
					continue;
				} catch (SystemException e1) {
					e1.printStackTrace();
					continue;
				}
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
			}
			if(f < reprate){//Current transaction will be single
				
//				int par = Integer.valueOf(txn) % 5 +1;
				int key = Integer.valueOf(txn);
				//int rq = Math.abs(rnd.nextInt()) % 5;
				int rq = tsize - 1;//Always select the last query to avoid congestion
				int rkey = Math.abs(rnd.nextInt()) % 10;
				if(key > (1000000 - 10 * tsize) ){
					key = key + rq - rkey * tsize;
//					key = key - rkey * 5;
				}else{
					key = key + rq + rkey * tsize;
				}
				int par = key % 5 + 1;
				for(int i=0;i<tsize;i++){
					if(w>this.write_rate){
						String fsql = "SELECT * from load WHERE id=" + String.valueOf(key);
						try {
							jta.query(fsql, par, QueryType.SELECT);
						} catch (SQLException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								//e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						} catch (InterruptedException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						}
					}else if(w<=this.write_rate){
						String fsql = "UPDATE load set data=" + String.valueOf(key) + " WHERE id=" + String.valueOf(key);
						try {
							jta.query(fsql, par, QueryType.UPDATE);
						} catch (SQLException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								//e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						} catch (InterruptedException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								//e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						}
					}
					//key += 5;
				}
			}else{
//				int key = Math.abs(rnd.nextInt()) % 100000;
				int key = Integer.valueOf(txn);
				for(int i=key;i<key+tsize;i++){
					
					int par = i % 5 + 1;
					
					if(w>this.write_rate){
						String fsql = "SELECT * from load WHERE id=" + String.valueOf(i);
						try {
							jta.query(fsql, par, QueryType.SELECT);
						} catch (SQLException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								//e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								//e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								//e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						} catch (InterruptedException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								//e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						}
					}else if(w<=this.write_rate){
						String fsql = "UPDATE load set data=" + String.valueOf(i) + " WHERE id=" + String.valueOf(i);
						try {
							jta.query(fsql, par, QueryType.UPDATE);
						} catch (SQLException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								//e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						} catch (InterruptedException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								//e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						}
					}
				}
			}
			
			try {
				jta.commit();
			} catch (SecurityException e) {
				e.printStackTrace();
				error = true;
				continue;
			} catch (IllegalStateException e) {
				//e.printStackTrace();
				error = true;
				continue;
			} catch (RollbackException e) {
				e.printStackTrace();
				error = true;
				continue;
			} catch (HeuristicMixedException e) {
				e.printStackTrace();
				error = true;
				continue;
			} catch (HeuristicRollbackException e) {
				e.printStackTrace();
				error = true;
				continue;
			} catch (SystemException e) {
				e.printStackTrace();
				error = true;
				continue;
			}
			if(error)
				continue;
			long lat = System.currentTimeMillis() - current;
			if(this.max_latency < lat)
				this.max_latency = lat;
			this.sum_latency += lat;
			count++;
		}
	}

	
	
	private void runGivenPiggyback(){
		Random rnd = new Random();
		rnd.setSeed(System.currentTimeMillis());
		
		while(true){
			boolean error = false;
			if(stop){
				synchronized(this){
					try {
						this.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

			long current = System.currentTimeMillis();
			String txn = null;
			if(step.hasNext()){
				txn = step.next();
			}else{
				step = history.iterator();
				txn = step.next();
			}
//			float f = rnd.nextFloat();
			float w = rnd.nextFloat();
			while(true){			
				
				try {
					jta.begin();
					if(jta.getStatus() != Status.STATUS_NO_TRANSACTION)
						break;
					//this.wait(1000);
				} catch (NotSupportedException e1) {
//					e1.printStackTrace();
					error = true;
					break;
				} catch (SystemException e1) {
//					e1.printStackTrace();
					error = true;
					break;
				}
			}
			if(this.txnSingle.get(txn)){//Current transaction will be single
				
//				int par = Integer.valueOf(txn) % 5 +1;
				int key = Integer.valueOf(txn);
				//int rq = Math.abs(rnd.nextInt()) % 5;
				int rq = tsize - 1;//Always select the last query to avoid congestion
				int rkey = Math.abs(rnd.nextInt()) % 10;
				if(key > (500000 - 10 * tsize) ){
					key = key + rq - rkey * tsize;
				}else{
					key = key + rq + rkey * tsize;
				}
				int par = key % 5 + 1;
				for(int i=0;i<tsize;i++){
					if(w>this.write_rate){
						String fsql = "SELECT * from load WHERE id=" + String.valueOf(key);
						try {
							jta.query(fsql, par, QueryType.SELECT);
						} catch (SQLException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						} catch (InterruptedException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						}
					}else if(w<=this.write_rate){
						String fsql = "UPDATE load set data=" + String.valueOf(key) + " WHERE id=" + String.valueOf(key);
						try {
							jta.query(fsql, par, QueryType.UPDATE);
						} catch (SQLException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
						} catch (InterruptedException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						}
					}
					//key += 5;
				}
			}else{
				
				
//				int key = Math.abs(rnd.nextInt()) % 100000;
				int key = Integer.valueOf(txn);
				for(int i=key;i<key+tsize;i++){
					
					int par = i % 5 + 1;
					
					if(w>this.write_rate){
						String fsql = "SELECT * from load WHERE id=" + String.valueOf(i);
						try {
							jta.query(fsql, par, QueryType.SELECT);
						} catch (SQLException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						} catch (InterruptedException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						}
					}else if(w<=this.write_rate){
						String fsql = "UPDATE load set data=" + String.valueOf(i) + " WHERE id=" + String.valueOf(i);
						try {
							jta.query(fsql, par, QueryType.UPDATE);
						} catch (SQLException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						} catch (InterruptedException e) {
							try {
								jta.abort();
							} catch (IllegalStateException e1) {
//								e1.printStackTrace();
								error = true;
								break;
							} catch (SecurityException e1) {
								e1.printStackTrace();
								error = true;
								break;
							} catch (SystemException e1) {
								e1.printStackTrace();
								error = true;
								break;
							}
							e.printStackTrace();
							error = true;
							break;
						}
					}
				}
				if(this.txnFreq.containsKey(txn) && this.txnFreq.get(txn) >= this.freqThreshold){
					int base = Integer.valueOf(txn);
					Integer f = this.txnFreq.get(txn);
					int par = base % 25;
					if(par < 5)
						par = 1;
					else if (par < 10)
						par = 2;
					else if (par < 15)
						par = 3;
					else if (par < 20)
						par = 4;
					else
						par = 5;
					
					this.prog += f;
					rep_amount++;
					
					for(int j=0;j<tsize;j++){
						if((base % 5 + 1) == par){
							continue;
						}
						else{
							if(f == 1){
								Load load = new Load();
								load.data = base;
								load.id = base;
								String readsql = load.getSelect();
								try {
									jta.query(readsql, base % 5 + 1, QueryType.SELECT);
								} catch (SQLException e) {
									e.printStackTrace();
									error = true;
									break;
								} catch (InterruptedException e) {
									e.printStackTrace();
									error = true;
									break;
								}
								
								String sql = load.getInsert();
								
								try {
									jta.query(sql, par, QueryType.INSERT);
								} catch (SQLException e) {
									e.printStackTrace();
									error = true;
									break;
								} catch (InterruptedException e) {
									e.printStackTrace();
									error = true;
									break;
								}
							}else if(f < 10){
								Load load = new Load();
								load.data = base;
								load.id = base;
								String readsql = load.getSelect();
								try {
									jta.query(readsql, base % 5 + 1, QueryType.SELECT);
								} catch (SQLException e) {
									e.printStackTrace();
									error = true;
									break;
								} catch (InterruptedException e) {
									e.printStackTrace();
									error = true;
									break;
								}
								for(int k=0;k<f;k++){
									
									load.data = base + k * 500000;
									load.id = load.data;
									String sql = load.getInsert();
									try {
										jta.query(sql, par, QueryType.INSERT);
									} catch (SQLException e) {
										e.printStackTrace();
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}
							}else{
								Load load = new Load();
								load.data = base;
								load.id = base;
								String readsql = load.getSelect();
								try {
									jta.query(readsql, base % 5 + 1, QueryType.SELECT);
								} catch (SQLException e) {
									e.printStackTrace();
									error = true;
									break;
								} catch (InterruptedException e) {
									e.printStackTrace();
									error = true;
									break;
								}
								for(int k=0;k<10;k++){
									load.data = base + k * 1000000;
									load.id = load.data;
									String sql = load.getInsert();
									try {
										jta.query(sql, par, QueryType.INSERT);
									} catch (SQLException e) {
										e.printStackTrace();
										error = true;
										break;
									} catch (InterruptedException e) {
										e.printStackTrace();
										error = true;
										break;
									}
								}
							}
						}
						base++;
					}
				}
			}
			
			try {
				jta.commit();
			} catch (SecurityException e) {
				e.printStackTrace();
				error = true;
				continue;
			} catch (IllegalStateException e) {
//				e.printStackTrace();
				error = true;
				continue;
			} catch (RollbackException e) {
				e.printStackTrace();
				error = true;
				continue;
			} catch (HeuristicMixedException e) {
				e.printStackTrace();
				error = true;
				continue;
			} catch (HeuristicRollbackException e) {
				e.printStackTrace();
				error = true;
				continue;
			} catch (SystemException e) {
				e.printStackTrace();
				error = true;
				continue;
			}
			if(error)
				continue;
			long lat = System.currentTimeMillis() - current;
			if(this.max_latency < lat)
				this.max_latency = lat;
			this.sum_latency += lat;
			count++;
		}
	}

	@Override
	public void run() {
		
		if(this.txnSingle != null)
			this.runGivenPiggyback();
		else
			runGiven();
	}
	
}
