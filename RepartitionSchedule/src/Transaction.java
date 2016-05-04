import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.Callable;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;




public class Transaction implements Callable<ResultSet> {

	Task task;
	JTA jta;
	int repno;
	//float reprate;
	boolean piggybacked = false;
	boolean ispiggyback = false;
	Random rnd;
	
	
	public Transaction(Task task, int repno, JTA jta){
		this.task = task;
		this.jta = jta;
		this.repno = repno;
	}
	
//	public Transaction(Task task, float reprate, JTA jta){
//		this.task = task;
//		this.jta = jta;
//		this.reprate = reprate;
//	}

	@SuppressWarnings("finally")
	@Override
	public ResultSet call(){
		long start = System.currentTimeMillis();
		rnd = new Random(start);
		ResultSet result = new ResultSet(task, false);
		java.sql.ResultSet rs;
		this.ispiggyback = task.isPiggyback;
		//Random rnd = new Random(System.currentTimeMillis());
		
//		int rtycount = 0;
		while(true){
			try {
				jta.begin();
				if(jta.getStatus() != Status.STATUS_NO_TRANSACTION)
					break;
				else
					this.wait(100);
				//this.wait(500);
//				rtycount++;
//				if(rtycount > 100)
//					return result;
				if(System.currentTimeMillis() - start > 120000)
					return result;
			} catch (NotSupportedException e1) {
				try {
					jta.abort();
				} catch (IllegalStateException e2) {
					e2.printStackTrace();
				} catch (SecurityException e2) {
					e2.printStackTrace();
				} catch (SystemException e2) {
					e2.printStackTrace();
				}finally{
					return result;
				}
			} catch (SystemException e1) {
				e1.printStackTrace();
				return result;
			} catch (InterruptedException e) {
				e.printStackTrace();
				return result;
			}
		}

		
		boolean isSingle = false;
		int writecount = 0;
		if(this.ispiggyback){
			if(this.task.getPiggy() == null || this.task.getPiggy() == this.task.id)
				if(!this.piggybackRep(start, this.task.id)){
					task.piggybacked = true;
					isSingle = true;
				}else{
					task.piggybacked = false;
					isSingle = false;
				}
			else{
				if(!this.piggybackRep(start, this.task.getPiggy())){
					task.piggybacked = true;
				}else{
					task.piggybacked = false;
					isSingle = false;
				}
			}
		}else{
			if(task.no == repno)
				isSingle = true;
			else
				isSingle = false;
		}
		
		if(this.ispiggyback && (this.task.getPiggy() == null || this.task.getPiggy() == this.task.id)){
			task.piggybacked = true;
			isSingle = true;
		}//For debug
		
		task.isSingle = isSingle;
		
		if(isSingle){
			
			int idx = Math.abs(rnd.nextInt() % this.task.query.size());
			String query = this.task.query.get(idx);
			String[] elem = query.split("\t");
			String key = elem[0];
			Integer par = Integer.valueOf(elem[1]);
			String sql = elem[2];
			
			boolean iswrite = true;
			if(sql.contains("SELECT"))
				iswrite = false;
			for(int i=0;i<this.task.query.size();i++){
				if(iswrite){
					try {
						writecount++;
						rs = jta.query(sql, par, JTA.QueryType.UPDATE);
						if(System.currentTimeMillis() - start > 120000){
							jta.abort();
							return result;
						}
					} catch (SQLException e) {
						try {
							jta.abort();
						} catch (IllegalStateException e1) {
							e1.printStackTrace();
						} catch (SecurityException e1) {
							e1.printStackTrace();
						} catch (SystemException e1) {
							e1.printStackTrace();
						}finally{
							return result;
						}
					} catch (InterruptedException e) {
						try {
							jta.abort();
						} catch (IllegalStateException e1) {
							e1.printStackTrace();
						} catch (SecurityException e1) {
							e1.printStackTrace();
						} catch (SystemException e1) {
							e1.printStackTrace();
						}finally{
							e.printStackTrace();
							return result;
						}
					} catch (IllegalStateException e) {
						e.printStackTrace();
						return result;
					} catch (SecurityException e) {
						e.printStackTrace();
						return result;
					} catch (SystemException e) {
						e.printStackTrace();
						return result;
					}
				}else{
					try {
						rs = jta.query(sql, par, JTA.QueryType.SELECT);
						if(System.currentTimeMillis() - start > 120000){
							jta.abort();
							return result;
						}
					} catch (SQLException e) {
						try {
							jta.abort();
						} catch (IllegalStateException e1) {
							e1.printStackTrace();
						} catch (SecurityException e1) {
							e1.printStackTrace();
						} catch (SystemException e1) {
							e1.printStackTrace();
						}finally{
							return result;
						}
					} catch (InterruptedException e) {
						try {
							jta.abort();
						} catch (IllegalStateException e1) {
							e1.printStackTrace();
						} catch (SecurityException e1) {
							e1.printStackTrace();
						} catch (SystemException e1) {
							e1.printStackTrace();
						}finally{
							e.printStackTrace();
							return result;
						}
					} catch (IllegalStateException e) {
						e.printStackTrace();
						return result;
					} catch (SecurityException e) {
						e.printStackTrace();
						return result;
					} catch (SystemException e) {
						e.printStackTrace();
						return result;
					}
				}
			}
			
		}else{
			for(String query:this.task.query){
				String[] elem = query.split("\t");
				String key = elem[0];
				Integer par = Integer.valueOf(elem[1]);
				String sql = elem[2];
				boolean iswrite = true;
				if(sql.contains("SELECT"))
					iswrite = false;
				if(iswrite){
					try {
						writecount++;
						rs = jta.query(sql, par, JTA.QueryType.UPDATE);
						if(System.currentTimeMillis() - start > 120000){
							jta.abort();
							return result;
						}
					} catch (SQLException e) {
						try {
							jta.abort();
						} catch (IllegalStateException e1) {
							e1.printStackTrace();
						} catch (SecurityException e1) {
							e1.printStackTrace();
						} catch (SystemException e1) {
							e1.printStackTrace();
						}finally{
							return result;
						}
					} catch (InterruptedException e) {
						try {
							jta.abort();
						} catch (IllegalStateException e1) {
							e1.printStackTrace();
						} catch (SecurityException e1) {
							e1.printStackTrace();
						} catch (SystemException e1) {
							e1.printStackTrace();
						}finally{
							e.printStackTrace();
							return result;
						}
					} catch (IllegalStateException e) {
						e.printStackTrace();
						return result;
					} catch (SecurityException e) {
						e.printStackTrace();
						return result;
					} catch (SystemException e) {
						e.printStackTrace();
						return result;
					}
				}else{
					try {
						rs = jta.query(sql, par, JTA.QueryType.SELECT);
						if(System.currentTimeMillis() - start > 120000){
							jta.abort();
							return result;
						}
					} catch (SQLException e) {
						try {
							jta.abort();
						} catch (IllegalStateException e1) {
							e1.printStackTrace();
						} catch (SecurityException e1) {
							e1.printStackTrace();
						} catch (SystemException e1) {
							e1.printStackTrace();
						}finally{
							return result;
						}
					} catch (InterruptedException e) {
						try {
							jta.abort();
						} catch (IllegalStateException e1) {
							e1.printStackTrace();
						} catch (SecurityException e1) {
							e1.printStackTrace();
						} catch (SystemException e1) {
							e1.printStackTrace();
						}finally{
							e.printStackTrace();
							return result;
						}
					} catch (IllegalStateException e) {
						e.printStackTrace();
						return result;
					} catch (SecurityException e) {
						e.printStackTrace();
						return result;
					} catch (SystemException e) {
						e.printStackTrace();
						return result;
					}
				}
			}
		}
		
		task.write_rate = (float) writecount / task.query.size();
		try {
			jta.commit();
		} catch (SecurityException e) {
			return result;
		} catch (IllegalStateException e) {
			return result;
		} catch (RollbackException e) {
			return result;
		} catch (HeuristicMixedException e) {
			return result;
		} catch (HeuristicRollbackException e) {
			return result;
		} catch (SystemException e) {
			return result;
		}
		result.successed = true;
		result.task.finishTime = System.currentTimeMillis();
		return result;
	}
	
	public void setPiggyback(){
		this.ispiggyback = true;
	}
	
	private boolean piggybackRep(long start, String id){
		
		java.sql.ResultSet rs;
		Integer base = Integer.valueOf(id);
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
	
		for(int j=0;j<JTA.tsize;j++){
			if((base % 5 + 1) == par){
				continue;
			}
			else{
				int key = base;// + Math.abs((rnd.nextInt() % task.frequency) * 1000000);
				Load load = new Load();
				load.data = base;
				load.id = base;
				String readsql = load.getSelect();
				try {
					rs = jta.query(readsql, base % 5 + 1, JTA.QueryType.SELECT);
					if(System.currentTimeMillis() - start > 120000){
						return true;
					}
				} catch (SQLException e) {
					return true;
				} catch (InterruptedException e) {
					return true;
				}
				
				load.data = key;
				load.id = key;
				String sql = load.getInsert();

				try {
					rs = jta.query(sql, par, JTA.QueryType.INSERT);
				} catch (SQLException e) {
					return true;
				} catch (InterruptedException e) {
					return true;
				}
			}
			base++;
		}
		
		return false;
	}
	
}
