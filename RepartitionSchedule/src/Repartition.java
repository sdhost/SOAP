import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.Callable;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;



public class Repartition implements Callable<ResultSet> {

	Task task;
	Task normalTask = null;
	JTA jta;
	Random rnd;
	
	public Repartition(Task task, JTA jta){
		this.task = task;
		this.jta = jta;
	}

	public void setPiggyback(Task task){
		this.normalTask = task;
	}
	
	@SuppressWarnings("finally")
	@Override
	public ResultSet call() throws Exception {
		long start = System.currentTimeMillis();
		rnd = new Random(start);
		ResultSet result = new ResultSet(task, false);
		java.sql.ResultSet rs;
		//Integer f = task.frequency;
		Integer base = Integer.valueOf(task.id);
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
					break;
				}
				else
					this.wait(100);
				
				if(System.currentTimeMillis() - start > 120000)
					return result;
			}
			
		} catch (NotSupportedException e) {
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
		} catch (SystemException e) {
			return result;
		} catch (InterruptedException e) {
			return result;
		}
		for(int j=0;j<JTA.tsize;j++){
			if((base % 5 + 1) == par){
				continue;
			}
			else{
				
				int key = base;
				if(task.frequency > 10){
					key = key + Math.abs((rnd.nextInt() % task.frequency) * 1000000);
				}
				Load load = new Load();
				load.data = base;
				load.id = base;
				String readsql = load.getSelect();
				try {
					rs = jta.query(readsql, base % 5 + 1, JTA.QueryType.SELECT);
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
						return result;
					}
				}
				load.data = key;
				load.id = key;
				String sql = load.getInsert();

				try {
					rs = jta.query(sql, par, JTA.QueryType.INSERT);
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
						return result;
					}
				}
			}
			base++;
		}
		try {
			jta.commit();
		} catch (SecurityException e) {
			return  result;
		} catch (IllegalStateException e) {
			return  result;
		} catch (RollbackException e) {
			return  result;
		} catch (HeuristicMixedException e) {
			return  result;
		} catch (HeuristicRollbackException e) {
			return  result;
		} catch (SystemException e) {
			return  result;
		}
		result.successed = true;
		return result;
		
	}
	
	

}
