import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class Simulator_gen {
	
	static TaskQueue taskQueue;
	static ExecutorService pool;
	static CompletionService<ResultSet> service;
	static JTA jta;
	static int rep_count = 0;
	static int tot_rep = 0;
	static int repno = -1;
	static float reprate = 0;
	static HashMap<Future<ResultSet>, Task> processing;
	static int submit_count = 0;

	
	static int type = 0;//Hard coding algorithm selector
	//Type 0: Apply all with highest priority
	//Type 1: Apply all with lowest priority
	//Type 2: Decided by contribution to rep_count
	//Type 3: Piggyback apply part
	//Type 4: Piggyback Apply all
	//Type -1: For debugging
	static int txn_size = 0;
	static double delta = 1.0/3.0;
	
	static LinkedList<String> history;
	static LinkedList<String> workload;
	static Map<String,LinkedList<String>> txnDef;
	static Map<String, Boolean> txnSingle;
	static Map<String, Integer> txnFreq;
	static LinkedList<Task> repTask = null;
	static LinkedList<Integer> load_dist = null;
	
	static Map<String, Integer> idmap;
	
	public static void main(String[] args) throws FileNotFoundException {
		
		String workload_file = "Test1WorkloadZipf@139056796";
		String poissdst = "PoissonRND";
		String workload_file_par = "Test1Workload.Repartition@139056797";
		
		try {
			init(100, workload_file, workload_file_par, poissdst);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		execApply(10,20);
		
		

	}
	
	private static int init(int thread_count, String workload_file, String workload_file_par, String poissdst) throws IOException, SQLException{
		int start = 0;
		int par = 5;
		Vector<Properties> inis = JTA.genBTMTest(par);
		 jta = new JTA(inis);
		//End of DB init
		
        System.out.println("Start Load Workload File");
        String filename = workload_file;
        FileReader infile = null;
        BufferedReader bin = null;
        String line = null;
        if(workload_file != null){
        
	        try {
	        	File f = new File(jta.workdir + filename);
	        		
	        	
				infile = new FileReader(f);
				bin = new BufferedReader(infile);
			} catch (FileNotFoundException e) {	
				e.printStackTrace();
			}
	        history = new LinkedList<String>();
	        workload = new LinkedList<String>();
			while((line = bin.readLine())!=null){
				workload.add(line);
				
			}
			
			
			
			bin.close();
			infile.close();
		
			System.out.println("Workload Loaded");
			
			
			txnDef = new HashMap<String, LinkedList<String>>();
			txnSingle = new LinkedHashMap<String, Boolean>();
			
			if(type == 5)
				txnFreq = new HashMap<String, Integer>();
			
			processWorkload(workload);
			
			System.out.println("Transaction Loaded: count = " + txnDef.size());
			//End of workload load
			System.out.println(" Workload Processed");
        }
		
		if(workload_file_par != null){
			try {
	        	File f = new File(jta.workdir + workload_file_par);
	        		
	        	
				infile = new FileReader(f);
				bin = new BufferedReader(infile);
			} catch (FileNotFoundException e) {	
				e.printStackTrace();
			}
			
			repTask = new LinkedList<Task>();
			idmap = new HashMap<String, Integer>();
			int i = 0;
			while((line = bin.readLine())!=null){
				String[] elem = line.split("\t");
				if(elem.length != 2){
					txn_size = Integer.valueOf(elem[0]);
					continue;
				}
				String id = elem[0];
				Integer freq = Integer.valueOf(elem[1]);
				tot_rep += freq;
				Task t = new Task(i, id, null, -1, System.currentTimeMillis(), freq);
				idmap.put(id, i);
				i++;
				repTask.add(t);
				if(type == 5)
					txnFreq.put(id, freq);
			}
			
			System.out.println(repTask.size() + " repartition task loaded");
			
			
			
			
			//Collections.shuffle(replist);
			
			bin.close();
			infile.close();
		}
		
		if(poissdst != null){
			try {
	        	File f = new File(jta.workdir + poissdst);
	        		
	        	
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
		
		pool = Executors.newFixedThreadPool(thread_count);
		service = new ExecutorCompletionService<ResultSet>(pool);
		taskQueue = new TaskQueue();
		processing = new HashMap<Future<ResultSet>, Task>();
		
		System.out.println("Initialized!");
		return start;
	}
	
	private static void processWorkload(LinkedList<String> workload) {
		String flag = null;
		LinkedList<String> queries = new LinkedList<String>();
		boolean error = false;
		String key = null;
		for(Iterator<String> it = workload.listIterator();it.hasNext();){
			String query = it.next();
			
			if(flag == null || flag.equalsIgnoreCase("COMMIT") || query.toLowerCase().contains("begin")){
				flag = query;
				error = false;
				continue;
			}else if(query.toLowerCase().contains("commit")){
				flag = "COMMIT";
				if(!error){
					
					history.addLast(key);

					txnDef.put(key, queries);
					//txnSingle.put(key, false);//TODO: for debug
					txnSingle.put(key, true);
					
					
					queries = new LinkedList<String>();
					
					key = null;
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
        			String sql = null,par = null, tkey = null;
        			tkey = elem[0];
        			if(key == null)
        				key = tkey;
        			par = elem[1];
        			sql = elem[2];
        			
        			queries.addLast(tkey + "\t" + par + "\t" + sql);

        		}
			}
		}

	}

	private static void execApply(int warmup_interval, int cooldown_interval) throws FileNotFoundException{
		long start = System.currentTimeMillis();
		long begin = start;
		int tot_executed = 0;
		int max = 17;//TODO: Need to change when doing the experiment to fit the actual data
		
		File f = new File(jta.workdir + "ResultType" + type + "T" + 100);
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
		
		thro.println("Throughput\tLoadScale\tLatency\tTxnCount\tRepCount\tRepRate\tSingleRate");
		//thro.println("Begin\t" + start);
		
		int counter = 0, finishc = 0;
		Iterator<String> workload = history.iterator();
		
		int tot = 60000;
		int int_amount = 3000;
		//rep_count = tot_rep;
		float act_single = 0;
		reprate = 0.65f;
		
		repTask.clear();//TODO: for debug
		
		for(Integer load: load_dist){
			int scale_load = load * 2;
			float scale;
			if(scale_load >= max)
				scale = 1;
			else
				scale = (float)scale_load / max;
			
			
			int amount = (int)(scale * int_amount);
			
			tot -= amount;
			if(tot < 0 && finishc == 0)
				amount += tot; 
			
			if(amount > 0){
				System.out.println(tot + " transactions are prepared to submit");
				System.out.println(amount + " txns will be add to queue");
				for(int i=0;i<amount;i++){
					String txn = null;
					if(workload.hasNext()){
						txn = workload.next();
					}else{
						workload = history.iterator();
						txn = workload.next();
					}
					Task newtask = new Task(idmap.get(txn), txn, txnDef.get(txn), 1, System.currentTimeMillis(), 0);
					taskQueue.put(newtask);
				}
			}
			System.out.println(taskQueue.size() + " transactions in queue");
			int rep_txn_count = 0;
			int txnCount = 0;
			long latency = 0;
			int single_count = 0;
			float writerate = 0;
			//int repCount = 0;
			
			while(System.currentTimeMillis() - start < 30000 && !taskQueue.isEmpty()){
				while(submit_count < 100 && !taskQueue.isEmpty()){
					Task t = taskQueue.poll();
					if(type == 5 && counter > warmup_interval){
						if(!txnSingle.get(t.id)){
							t.isPiggyback = true;
							t.frequency = txnFreq.get(t.id);
							txnSingle.put(t.id, true);
						}
					}
					if(t.isPiggyback){
						Transaction txn = new Transaction(t, reprate, jta);
						Future<ResultSet> result = service.submit(txn);
						processing.put(result, t);
						//rep_txn_count++;
					}else if(t.frequency > 0){
						Repartition rep = new Repartition(t, jta);
						Future<ResultSet> result = service.submit(rep);
						processing.put(result, t);
						//rep_txn_count++;
					}else{
						Transaction txn;
//						if(txnSingle.get(t.id))
//							txn = new Transaction(t, t.no, jta);
//						else
//							txn = new Transaction(t, -1, jta);
						txn = new Transaction(t, reprate, jta);//TODO: for debug
						
						Future<ResultSet> result = service.submit(txn);
						processing.put(result, t);
					}
					submit_count++;
				}
				
				
				if(submit_count >= 100){
					LinkedList<Future<ResultSet>> results = new LinkedList<Future<ResultSet>>();
					Future<ResultSet> fresult = null;
					try {
						while((fresult = service.poll(100, TimeUnit.MILLISECONDS)) != null){
							results.add(fresult);
						}
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					for(Future<ResultSet> result:results){
						if(result != null){
							submit_count--;
							if(result.isCancelled()){
								if(processing.containsKey(result))
									taskQueue.put(processing.get(result));
								System.out.println("txn cancelled");
							}else if(result.isDone()){
								processing.remove(result);
								//processing.remove(result);
								//System.out.println("txn done");
								
								try {
									ResultSet r = result.get();
									boolean isrep = (r.task.frequency > 0);
									boolean ispiggyback = r.task.isPiggyback;
									if(!r.successed){
										if(isrep)//Include piggyback and normal repartition txns
											taskQueue.put(r.task);
									}else{
										if(isrep){
											rep_count += r.task.frequency;
											reprate = (float)rep_count / tot_rep;
											rep_txn_count++;
											
											txnSingle.put(r.task.id, true);
											if(ispiggyback){
												txnCount += 1;
												single_count += 1;
												latency += System.currentTimeMillis() - r.task.timestamp;
												writerate += r.task.write_rate;
											}
											if(repno < r.task.no)
												repno = r.task.no;
										}else{
											if(r.task.isSingle)
												single_count++;
											writerate += r.task.write_rate;
											txnCount += 1;
											latency += System.currentTimeMillis() - r.task.timestamp;
										}
											
									}
										
								} catch (InterruptedException e) {
									e.printStackTrace();
								} catch (ExecutionException e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			}
			
			System.out.println(rep_txn_count + " repartition transaction are submitted");
			
			float exec_scale = (float)(System.currentTimeMillis() - start) / 60000;
			
			
			
			//System.out.println(processing.size() + " txns are currently processing");
			
			//rep_count += repCount;
			float lat = (float)latency / txnCount;
			
			counter += 1;
			
			System.out.println("Executed " + (float) counter / 2 + " minutes");
			if(counter > warmup_interval){
				//Start repartition task adding
				if(rep_count < tot_rep){
					if(repTask.isEmpty()){
						
					}else{
						if(type == 0){//Apply all with higher priority than normal txn
							for(Task t:repTask){
								t.priority = t.frequency + 1;
								taskQueue.put(t);
							}
							System.out.println(repTask.size() + " repartition txns submitted");
							repTask.clear();
						}else if(type == 1){//Apply all with lower priority than normal txn
							for(Task t:repTask){
								t.priority = 0;
								taskQueue.put(t);
							}
							repTask.clear();
						}else if(type == 2){//Apply all with dynamic priority
							LinkedList<Task> submitted = new LinkedList<Task>();
							float throughput = txnCount / exec_scale;
							int toRep = (int) Math.floor(throughput * delta);
							if(toRep > 2000)
								toRep = 2000;
							int i = 0;
							System.out.println(toRep + " repartition txns submitted");
							for(Task t:repTask){
								i++;
								t.priority = t.frequency + 1;
								taskQueue.put(t);
								submitted.add(t);
								if(i > toRep)
									break;
							}
							repTask.removeAll(submitted);
						}else if(type == 4){
							//Piggyback solution
							for(Task t:repTask){
								t.priority = t.frequency;
								t.query = txnDef.get(t.id);
								t.isPiggyback = true;
								taskQueue.put(t);
							}
							repTask.clear();
						}else if(type == 3){
							//Piggyback solution
							LinkedList<Task> submitted = new LinkedList<Task>();
							float throughput = txnCount / exec_scale;
							int toRep = (int) Math.floor(throughput * delta);
							if(toRep > 2000)
								toRep = 2000;
							int i = 0;
							System.out.println(toRep + " repartition txns submitted");
							for(Task t:repTask){
								i++;
								t.priority = t.frequency + 1;
								t.query = txnDef.get(t.id);
								t.isPiggyback = true;
								taskQueue.put(t);
								submitted.add(t);
								if(i > toRep)
									break;
							}
							repTask.removeAll(submitted);
						}else if(type == -1){//For debug type
							for(Task t:repTask){
								t.priority = t.frequency;
								taskQueue.put(t);
								if(t.frequency < 20)
									break;
							}
							repTask.clear();
						}else if(type == 5){//Piggyback without inserting repartition queries;
							//Do nothing here
						}
						else{
							System.out.println("Unsupported Algorithm for type = " + type);
						}
					}
				}else{
					if(!repTask.isEmpty()){
						System.out.println(repTask.size() + " repartition txns submitted");
						for(Task t:repTask){
							t.priority = t.frequency + 1;
							taskQueue.put(t);
						}
						repTask.clear();
					}
					if(taskQueue.isEmpty()){
						finishc += 1;
						if(finishc > cooldown_interval)
							break;
					}
				}
			}
			
			if(System.currentTimeMillis() - start < 30000 && taskQueue.isEmpty()){
//				System.out.println("Sleeping " + (60000 + start - System.currentTimeMillis()) + " ms");
				//exec_scale = (float)(System.currentTimeMillis() - start) / 60000;
//				try {
//					Thread.sleep(60000 + start - System.currentTimeMillis());
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
			}else{
//				while(System.currentTimeMillis() - start < 60000 && !taskQueue.isEmpty()){
//					while(submit_count < 100 && !taskQueue.isEmpty()){
//						Task t = taskQueue.poll();
//						if(type == 5 && counter > warmup_interval){
//							if(!txnSingle.get(t.id)){
//								t.isPiggyback = true;
//								t.frequency = txnFreq.get(t.id);
//								txnSingle.put(t.id, true);
//							}
//						}
//						if(t.isPiggyback){
//							Transaction txn = new Transaction(t, reprate, jta);
//							Future<ResultSet> result = service.submit(txn);
//							processing.put(result, t);
//							rep_txn_count++;
//						}else if(t.frequency > 0){
//							Repartition rep = new Repartition(t, jta);
//							Future<ResultSet> result = service.submit(rep);
//							processing.put(result, t);
//							rep_txn_count++;
//						}else{
//							Transaction txn;
//							if(txnSingle.get(t.id))
//								txn = new Transaction(t, t.no, jta);
//							else
//								txn = new Transaction(t, -1, jta);
//							
//							Future<ResultSet> result = service.submit(txn);
//							processing.put(result, t);
//						}
//						submit_count++;
//					}
//					
//					
//					if(submit_count >= 100){
//						LinkedList<Future<ResultSet>> results = new LinkedList<Future<ResultSet>>();
//						Future<ResultSet> fresult = null;
//						try {
//							while((fresult = service.poll(100, TimeUnit.MILLISECONDS)) != null){
//								results.add(fresult);
//							}
//						} catch (InterruptedException e1) {
//							e1.printStackTrace();
//						}
//						for(Future<ResultSet> result:results){
//							if(result != null){
//								submit_count--;
//								if(result.isCancelled()){
//									if(processing.containsKey(result))
//										taskQueue.put(processing.get(result));
//									//System.out.println("txn cancelled");
//								}else if(result.isDone()){
//									processing.remove(result);
//									//processing.remove(result);
//									//System.out.println("txn done");
//									
//									try {
//										ResultSet r = result.get();
//										boolean isrep = (r.task.frequency > 0);
//										boolean ispiggyback = r.task.isPiggyback;
//										if(!r.successed){
//											if(isrep)//Include piggyback and normal repartition txns
//												taskQueue.put(r.task);
//										}else{
//											if(isrep){
//												rep_count += r.task.frequency;
//												
//												txnSingle.put(r.task.id, true);
//												if(ispiggyback){
//													txnCount += 1;
//													single_count += 1;
//													latency += System.currentTimeMillis() - r.task.timestamp;
//													writerate += r.task.write_rate;
//												}
//												if(reprate < r.task.no)
//													reprate = r.task.no;
//											}else{
//												if(r.task.isSingle)
//													single_count++;
//												writerate += r.task.write_rate;
//												txnCount += 1;
//												latency += System.currentTimeMillis() - r.task.timestamp;
//											}
//												
//										}
//											
//									} catch (InterruptedException e) {
//										e.printStackTrace();
//									} catch (ExecutionException e) {
//										e.printStackTrace();
//									}
//								}
//							}
//						}
//					}
//				}
			}
			
			exec_scale = (float)(System.currentTimeMillis() - start) / 60000;
			
			System.out.println("Executed: " + txnCount + " txns, " + single_count + " are single; " + (float) single_count / txnCount + "\t" + writerate / txnCount);
			System.out.println("Reprate: " + rep_count + "/" + tot_rep + "\t" + reprate);
			System.out.println("Throughput: " + String.valueOf(txnCount / exec_scale));
			System.out.println("LatencyAvg: " + String.valueOf((double)lat));
			thro.println(String.valueOf(txnCount / exec_scale) + "\t"
						+ scale + "\t"
						+ (double)lat + "\t"
						+ txnCount + "\t"
						+ rep_txn_count + "\t"
						+ (float)rep_count / tot_rep + "\t"
						+ (float) single_count / txnCount);
			tot_executed += txnCount;
			
			start = System.currentTimeMillis();
			act_single += (float) single_count / txnCount;
			//TODO: for debug
			if(counter > cooldown_interval)
				break;
			
		}
		
		thro.println("End\t" + (float)tot_executed /(System.currentTimeMillis() - begin) * 60000);
		System.out.println("End\t" + (float)tot_executed /(System.currentTimeMillis() - begin) * 60000);
		System.out.println("Single\t" + act_single / counter);
		
		thro.close();
		try {
			performance_out.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println("Shutting down the executor");
		pool.shutdown();
		
		try {
			pool.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("All Finish!");
		
	}
	
	
	
	}
