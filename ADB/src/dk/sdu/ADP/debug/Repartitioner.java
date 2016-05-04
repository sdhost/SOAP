package dk.sdu.ADP.debug;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;

import dk.sdu.ADP.ADP.QueryType;
import dk.sdu.ADP.JTA.JTA;
import pojo.Load;

public class Repartitioner implements Runnable{

	List<String> history;
	JTA jta;
	boolean stop = false;
	Load load;
	int progress;
	int total_prog;
	int start;
	int total;
	final int PAR = 5;
	private long interval = 0;
	long[] last_wait;
	long max_wait;
	int tsize = 5;
	
	int type = -1;
	
	public Repartitioner(List<String> list, int total, JTA jta, int tsize){
		this.jta = jta;
		this.history =  list;
		this.total = total;
		this.progress = 0;
		this.last_wait = new long[PAR+1];
		for(int i=1;i<=PAR;i++)
			last_wait[i]=0;
		this.max_wait = 10000;
		this.tsize = tsize;
	}
	
	public Repartitioner(List<String> list, int total, JTA jta) {
		this.jta = jta;
		this.history =  list;
		this.total = total;
		this.progress = 0;
		this.last_wait = new long[PAR+1];
		for(int i=1;i<=PAR;i++)
			last_wait[i]=0;
		this.max_wait = 10000;
	}

	public void stopThread(){
		this.stop = true;
	}
	
	public void setType(int type){
		this.type = type;
	}
	
	public double getProgress(){
//		return (double)(this.progress - this.start) / (double)(this.total - this.start);
		if(this.progress == this.total_prog)
			return 2.0d;
		return (double)(this.progress / (double) this.total_prog);
	}
	
//	private void runAllRandom(){
//		this.stop = false;
////		long start = System.currentTimeMillis();
//		while(progress < total){
////			long current = System.currentTimeMillis();
//			if(stop){
//				synchronized(this){
//					try {
//						this.wait();
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
////				start = System.currentTimeMillis();
//			}
//			try {
//				jta.begin();
//			} catch (NotSupportedException e) {
//				e.printStackTrace();
//			} catch (SystemException e) {
//				e.printStackTrace();
//			}
//			
//			int par = progress % 25;
//			if(par < 5)
//				par = 1;
//			else if (par < 10)
//				par = 2;
//			else if (par < 15)
//				par = 3;
//			else if (par < 20)
//				par = 4;
//			else
//				par = 5;
//			for(int i=1;i<=tsize;i++){
//				if(i==par){
//					progress++;
//					continue;
//				}else{
//					Load load = new Load();
//					load.id = progress;
//					load.data = progress;
//					try {
//						jta.query(load.getInsert(), par, QueryType.INSERT);
//					} catch (SQLException e) {
//						try {
//							jta.abort();
//						} catch (IllegalStateException e1) {
//							e1.printStackTrace();
//						} catch (SecurityException e1) {
//							e1.printStackTrace();
//						} catch (SystemException e1) {
//							e1.printStackTrace();
//						}
//						e.printStackTrace();
//					} catch (InterruptedException e) {
//						try {
//							jta.abort();
//						} catch (IllegalStateException e1) {
//							e1.printStackTrace();
//						} catch (SecurityException e1) {
//							e1.printStackTrace();
//						} catch (SystemException e1) {
//							e1.printStackTrace();
//						}
//						e.printStackTrace();
//					}
//					progress++;
////					if(progress > total){
////						stop = true;
////					}
//				}
//			}
//			
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
//			
//		}
//	}
//	
	private void runGivenWorkload(){
		if(this.progress >= this.total_prog)
			return;
		for(int i=start;i<total;i++){
			
			if(stop){
				synchronized(this){
					try {
						this.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
			String line = history.get(i);
			String elem[] = line.split("\t");
			Integer base = Integer.valueOf(elem[0]);
			Integer f = Integer.valueOf(elem[1]);
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
			
			try {
				while(true){
					jta.begin();
					if(jta.getStatus() != Status.STATUS_NO_TRANSACTION){
						if(type == 2)
							last_wait[par] = last_wait[par] / 2;
						break;
					}else{
						if(type == 2 && last_wait[par] < max_wait){
							last_wait[par] += 1000;
							this.wait(last_wait[par]);
						}else{
							this.wait(5000);
						}
					}
				}
				
			} catch (NotSupportedException e) {
				e.printStackTrace();
			} catch (SystemException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
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
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						
						String sql = load.getInsert();
						
						try {
							jta.query(sql, par, QueryType.INSERT);
						} catch (SQLException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
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
						} catch (InterruptedException e) {
							e.printStackTrace();
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
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						for(int k=0;k<10;k++){
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
					}
				}
				base++;
			}
			try {
				jta.commit();
				synchronized(this){
					this.notifyAll();
				}
			} catch (SecurityException e) {
				try {
					jta.abort();
				} catch (IllegalStateException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (SystemException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			} catch (IllegalStateException e) {
				try {
					jta.abort();
				} catch (IllegalStateException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (SystemException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			} catch (RollbackException e) {
				try {
					jta.abort();
				} catch (IllegalStateException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (SystemException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			} catch (HeuristicMixedException e) {
				try {
					jta.abort();
				} catch (IllegalStateException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (SystemException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			} catch (HeuristicRollbackException e) {
				try {
					jta.abort();
				} catch (IllegalStateException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (SystemException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			} catch (SystemException e) {
				try {
					jta.abort();
				} catch (IllegalStateException e1) {
					e1.printStackTrace();
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (SystemException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
			
			this.progress += f;
			
			if(type == 1)
				this.interval--;
			if(type == 1 && this.interval <= 0){
				try {
					synchronized(this){
						this.wait();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			
		}
		
		this.progress = this.total_prog;
	}
	
	@Override
	public void run() {
		
		//runAllRandom();
		runGivenWorkload();
		
	}

	public void setInterval(long deadline) {
//		if(this.interval > 0)
//			this.interval += deadline;
//		else
			this.interval = deadline;
		
	}

	public void setStart(int i) {
		this.start = i;
		this.progress = 0;
		this.total_prog = 0;
		for(int j=i;j<total;j++){
			String line = history.get(j);
			String elem[] = line.split("\t");
			Integer f = Integer.valueOf(elem[1]);                       
			this.total_prog += f;
		}
	}
	
	
}
