import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class Simulator_HybridWithFBAA_PiggybackPercent {
	
	
	static TaskQueue taskQueue;
	static ExecutorService pool;
	static CompletionService<ResultSet> service;
	static JTA jta;
	static int rep_count = 0;
	static int tot_rep = 0;
	static int repno = -1;
	static float reprate = 0.0f;
	static HashMap<Future<ResultSet>, Task> processing;
	static int submit_count = 0;

	static float preprog = 0f;//Starting progress
	// Should be within [0,1)
	
	static int workloadType = 0;//Workload Type selector
	//Type 0: Zipf Workload
	//Type 1: Uniform Workload
	static int type = 5;//Hard coding algorithm selector
	//Type 0: Apply all with highest priority
	//Type 1: Apply all with lowest priority
	//Type 2: Decided by contribution to rep_count
	//Type 3: Feedback with AfterAll
	//Type 4: Piggyback Hybrid Feedback
	//Type 5: Piggyback Passive
	//Type 6: Piggyback Hybrid type 3
	//Type -1: For debugging
	static double piggy_percent = 0.5;
	static int txn_size = 0;
	static double delta = 1.05;
	//static float MaxThroughput = 0;
	static int int_amount = 0;
	//static double dump_rate = 0.25;
	//static float single_factor = 7.0f;
	
	static LinkedList<String> history;
	static LinkedList<String> workload;
	static Map<String,LinkedList<String>> txnDef;
	static Map<String, Boolean> txnSingle;
	static Map<String, Integer> txnFreq;
	static LinkedList<Task> repTask = null;
	static LinkedList<Integer> load_dist = null;
	
	static Map<String, Integer> idmap;
	
	static int statInterval = 20000;
	
	public static void main(String[] args) throws FileNotFoundException {
		
		
		if(args != null){
			if(args.length == 3){
				preprog = Float.valueOf(args[0]);
				workloadType = Integer.valueOf(args[1]);
				type = Integer.valueOf(args[2]);
				System.out.println("Preprog: " + preprog + "\tWorkloadType: " + workloadType + "\tType: " + type);
			}else if(args.length == 4){
				preprog = Float.valueOf(args[0]);
				workloadType = Integer.valueOf(args[1]);
				if(args[2].equals("T")){
					delta  = Float.valueOf(args[3]);
					System.out.println("Preprog: " + preprog + "\tWorkloadType: " + workloadType + "\tDelta: " + delta);
				}else{
					type = Integer.valueOf(args[2]);
					delta = Float.valueOf(args[3]);
					System.out.println("Preprog: " + preprog + "\tWorkloadType: " + workloadType + "\tType: " + type + "\tDelta" + delta);
				}
			}else if(args.length == 5){
				preprog = Float.valueOf(args[0]);
				workloadType = Integer.valueOf(args[1]);
				type = Integer.valueOf(args[2]);
				delta = Float.valueOf(args[3]);
				int_amount = Integer.valueOf(args[4]);
				System.out.println("Preprog: " + preprog + "\tWorkloadType: " + workloadType + "\tType: " + type + "\tDelta" + delta);
				System.out.println("Maximum Interval Load:" + int_amount);
				
			}else if(args.length == 6){
				preprog = Float.valueOf(args[0]);
				workloadType = Integer.valueOf(args[1]);
				type = Integer.valueOf(args[2]);
				delta = Float.valueOf(args[3]);
				int_amount = Integer.valueOf(args[4]);
				piggy_percent = Double.valueOf(args[5]);
				System.out.println("Preprog: " + preprog + "\tWorkloadType: " + workloadType + "\tType: " + type + "\tDelta" + delta);
				System.out.println("Piggyback accuracy: " + piggy_percent);
				System.out.println("Maximum Interval Load:" + int_amount);
				
			}
		}
		String workload_file;
		String poissdst = "PoissonRND";
		String workload_file_par;
		if(workloadType == 0){
			workload_file = "Test1WorkloadZipf@139056796";
			workload_file_par = "Test1Workload.Repartition@139056797";
			//MaxThroughput = 25000;
			if(int_amount == 0)
				int_amount = 3000;
//			int_amount = 3600;
			//single_factor = 5.0f;
		}else if(workloadType == 1){
			workload_file = "Test1Workload@139273091";
			workload_file_par = "Test1Workload.Repartition@139273091";
			//MaxThroughput = 25000;
			if(int_amount == 0)
				int_amount = 3000;
//			int_amount = 7350;
			//single_factor = 7.0f;
		}else{
			System.out.println("Unsupported workload type");
			return;
		}
		
		try {
			init(100, workload_file, workload_file_par, poissdst);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		execApply(10,10);
		
		

	}
	
	private static int init(int thread_count, String workload_file, String workload_file_par, String poissdst) throws IOException, SQLException{
		int start = 0;
		int par = 5;
		Vector<Properties> inis = JTA.genBTMTest(par);
		 jta = new JTA(inis);
		//End of DB init
		
//        System.out.println("Start Load Workload File");
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
		
//			System.out.println("Workload Loaded");
			
			
			txnDef = new HashMap<String, LinkedList<String>>();
			txnSingle = new LinkedHashMap<String, Boolean>();
			
			txnFreq = new HashMap<String, Integer>();
			
			processWorkload(workload);
			
//			System.out.println("Transaction Loaded: count = " + txnDef.size());
			//End of workload load
//			System.out.println(" Workload Processed");
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
			idmap = new LinkedHashMap<String, Integer>();
			int i = 1;
			while((line = bin.readLine())!=null){
				String[] elem = line.split("\t");
				if(elem.length != 2){
					txn_size = Integer.valueOf(elem[0]);
					continue;
				}
				//Integer iid = Integer.valueOf(elem[0]);
				String id = elem[0];
				//if(iid >= 150000)
				//	continue;
				//else
				//id = String.valueOf(iid);
				Integer freq = Integer.valueOf(elem[1]);
				//if(txnFreq.get(id) != freq && !txnFreq.containsKey(id))
				//	txnFreq.put(id, freq);
				//else{
				//	freq = txnFreq.get(id);
				//}
				tot_rep += freq;
				Task t = new Task(i, id, null, -1, System.currentTimeMillis(), freq);
				idmap.put(id, i);
				i++;
				repTask.add(t);
				if(txnSingle.get(id) || !txnSingle.containsKey(id))
					System.out.println("Error Loading transactions");
				txnFreq.put(id, freq);
			}
			int max = 0;
			for(String txnk:txnFreq.keySet()){
				if(txnFreq.get(txnk) > max)
					max = txnFreq.get(txnk);
			}
			System.out.println("Max freq = " + max);
			
			if(preprog > 0){
				for(String id:idmap.keySet()){
					rep_count += txnFreq.get(id);
					txnSingle.put(id, true);
					if((float)rep_count/(float)tot_rep >preprog)
						break;
				}
				Set<Task> preprocessed = new HashSet<Task>();
				for(Task t: repTask){
					String id = t.id;
					if(txnSingle.get(id) == true)
						preprocessed.add(t);
				}
				repTask.removeAll(preprocessed);
//				System.out.println(preprocessed.size() + " txns are removed");
			}
			
//			System.out.println(repTask.size() + " repartition task loaded");
			
			
			
			if(workloadType == 1)
				Collections.shuffle(repTask, new Random(System.currentTimeMillis()));
			
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
			Collections.shuffle(load_dist, new Random(System.currentTimeMillis()));
			bin.close();
			infile.close();
		}
		
		pool = Executors.newFixedThreadPool(thread_count);
		service = new ExecutorCompletionService<ResultSet>(pool);
		taskQueue = new TaskQueue();
		processing = new HashMap<Future<ResultSet>, Task>();
		
//		System.out.println("Initialized!");
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
					txnSingle.put(key, false);
					
					//if(txnFreq.containsKey(key)){
					//	txnFreq.put(key, txnFreq.get(key)+1);
					//}else{
					//	txnFreq.put(key, 1);
					//}
					
					//txnSingle.put(key, true);
					
					
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
        			//tkey = String.valueOf(Integer.valueOf(elem[0]) % 150000);//Limit the transaction number to 30000
        			tkey = elem[0];
        			if(key == null)
        				key = tkey;
        			par = elem[1];
        			sql = elem[2];
        			
        			queries.addLast(tkey + "\t" + par + "\t" + sql);

        		}
			}
		}
		
		
		
		Collections.shuffle(history, new Random(System.currentTimeMillis()));
		workload.clear();
		workload = null;

	}

	private static void execApply(int warmup_interval, int cooldown_interval) throws FileNotFoundException{
		long start = System.currentTimeMillis();
		long begin = start;
		int max = 17;//It's 17 in Zipf workload
		if(workloadType==0)
			max = 17;
		
		int tot_txn = 0;
		
		File f = new File(jta.workdir + "Result" + type + "Workload" + workloadType + "Percent" + piggy_percent);
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
		
		String substr = "_T" + String.valueOf(type) + "W" + String.valueOf(workloadType) + "P" + String.valueOf(Integer.valueOf((int) Math.ceil(piggy_percent * 10)));
		thro.println("Throughput"+substr+
					 //"\tLoadScale"+substr+
					 "\tLatency"+substr+
					 "\tTxnCount"+substr+
					 "\tRepCount"+substr+
					 "\tRepRate" + substr+
					 "\tFailureRep" + substr+
					 "\tFailureCount" + substr);
					 //"\tSingleRate" + substr);
		
		//thro.println("Begin\t" + start);
		
		boolean first_time = true;//For Feedback-AfterAll type=3 method
		float last_exec = 0.33f;
		int last_rep = 0;
		int last_nor = 0;
		
		int counter = 0, finishc = 0;
		int tCounter = 0;
		Iterator<String> his_itr = history.iterator();
		float last_txn = 0;
		//boolean last_piggyback = true;
		
		int tot = 21000000;
		//int int_amount = 1800;
//		if(type == 5 || type == 3)
//			repTask.clear();
		
		int max_rep = 0;
		if(workloadType == 0)
			max_rep = 300;
		else if(workloadType == 1)
			max_rep = 300;
		
		
		for(Integer load: load_dist){
			if(System.currentTimeMillis() - begin > 3600000)
				break;
			
			int scale_load = load * 2;
			if(load==0)
				scale_load = 1;
			float scale;
			if(scale_load >= max)
				scale = 1;
			else
				scale = (float)scale_load / max;
			
			
			int amount = (int)(scale * int_amount * (statInterval / 60000.0));
			
			
			tot -= amount;
			
			if(amount > 0){
//				System.out.println(tot + " transactions are prepared to submit");
//				System.out.println(amount + " txns will be add to queue");
				for(int i=0;i<amount;i++){
					String txn = null;
					if(his_itr.hasNext()){
						txn = his_itr.next();
					}else{
						his_itr = history.iterator();
						txn = his_itr.next();
					}
					Task newtask = new Task(idmap.get(txn), txn, txnDef.get(txn), 1, System.currentTimeMillis(), 0);
					taskQueue.put(newtask);
				}
			}
//			System.out.println(taskQueue.size() + " transactions in queue");
			int rep_txn_count = 0;
			int if_piggy = 0;
			int piggy = 0;
			int txnCount = 0;
			int failTxnCount = 0;
			int failRepCount = 0;
			long latency = 0;
			long normal_end = 0;//For AfterAll method, get the finish time of normal transactions
			long normal_start = 0;
			int single_count = 0;
			float writerate = 0;
			//int repCount = 0;
			
			while(System.currentTimeMillis() - start < statInterval && !taskQueue.isEmpty()){
				while(submit_count < 100 && !taskQueue.isEmpty()){
					Task t = taskQueue.peek();
					if(t.priority == 1){
						if(t.frequency == 0 && normal_start == 0)
							normal_start = System.currentTimeMillis();
					}else if(t.priority < 0){
						//Only For type 1, type 3, type 6
						if(submit_count >= 10){//For type 1,3,6
							break;
						}
						else{
							if(normal_end == 0)
								normal_end = System.currentTimeMillis();
							if(txnSingle.get(t.id)==true){
								taskQueue.poll();
								continue;
							}
						}
					}
					
					taskQueue.poll();
					

						
					if(((type == 5)||(type == 4)||(type == 6)) && counter > warmup_interval){
						
						if(txnSingle.get(t.id) == false){
							if(t.frequency > 0){
								t.isPiggyback = false; //Feedback version of RepTxn
								//t.frequency = txnFreq.get(t.id);
							}else{
								Random rnd = new Random(System.currentTimeMillis());
								if(rnd.nextFloat() > piggy_percent){
									Task rndt = repTask.poll();
									while(rndt != null && txnSingle.get(rndt.id)){
										if(repTask.isEmpty())
											rndt = null;
										else
											rndt = repTask.poll();
										
									}
									if(rndt==null)
										t.frequency = txnFreq.get(t.id);
									else{
										t.setPiggy(rndt.id);
										t.frequency = txnFreq.get(rndt.id);
									}
								}else{
									t.frequency = txnFreq.get(t.id);
								}
								t.isPiggyback = true;
								
							}
						}
					}
					if(t.isPiggyback && txnSingle.get(t.id)==false){
						//if(t.query == null)
						//	t.query = txnDef.get(t.id);
						Transaction txn = new Transaction(t, repno, jta);
						Future<ResultSet> result = service.submit(txn);
						processing.put(result, t);
						submit_count++;
						//rep_txn_count++;
					}else if(t.frequency > 0 && txnSingle.get(t.id)==false){
						
						Repartition rep = new Repartition(t, jta);
						Future<ResultSet> result = service.submit(rep);
						processing.put(result, t);
						submit_count++;
						//rep_txn_count++;
					}else{
						if(t.query == null)
							t.query = txnDef.get(t.id);
						t.frequency = 0;
						t.isPiggyback = false;
						Transaction txn;
						if(txnSingle.get(t.id) == true)
							txn = new Transaction(t, t.no, jta);
						else
							txn = new Transaction(t, -1, jta);
						
						Future<ResultSet> result = service.submit(txn);
						processing.put(result, t);
						submit_count++;
					}
					
				}
				
				
				if(submit_count > 0 || !processing.isEmpty()){
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
//								System.out.println("txn cancelled");
							}else if(result.isDone()){
								processing.remove(result);
								//processing.remove(result);
								//System.out.println("txn done");
								
								try {
									ResultSet r = result.get();
									boolean isrep = (r.task.frequency > 0);
									boolean ispiggyback = r.task.isPiggyback;
									if(!r.successed){
										failTxnCount++;
										if(isrep){//Include piggyback and normal repartition txns
											failRepCount++;
											if(!txnSingle.get(r.task.id)){
												if(r.task.priority<=1){
													if(!r.task.isPiggyback){
														r.task.timestamp = System.currentTimeMillis();
														taskQueue.put(r.task);
													}else{
														r.task.timestamp = System.currentTimeMillis();
														r.task.isPiggyback = false;
														r.task.frequency = 0;
														r.task.piggyback_id = null;
														taskQueue.put(r.task);
													}
												}else{
													repTask.add(r.task);
												}
											}
										}else{
											r.task.timestamp = System.currentTimeMillis();
											taskQueue.put(r.task);
										}
									}else{
										if(isrep && !ispiggyback){
											if(!txnSingle.get(r.task.id)){
												rep_count += r.task.frequency;
												reprate = (float)rep_count / (float)tot_rep;
												rep_txn_count++;
												
												txnSingle.put(r.task.id, true);
												
												if(repno < r.task.no)
													repno = r.task.no;
											}
										}else if(isrep && ispiggyback){
											//if(normal_start == 0)
											//	normal_start = System.currentTimeMillis();
											if(r.task.getPiggy() != null){
												if(!r.task.piggybacked){
													// Cannot be this situation
												}else{
													piggy++;
													rep_count += r.task.frequency;
													reprate = (float)rep_count / (float)tot_rep;
													rep_txn_count++;
													
													txnSingle.put(r.task.getPiggy(), true);
													txnCount += 1;
													single_count += 1;
													latency += r.task.finishTime - r.task.timestamp;
													writerate += r.task.write_rate;
													if(repno < r.task.no)
														repno = r.task.no;
												}
											}
											else if(!txnSingle.get(r.task.id)){
												if(!r.task.piggybacked){
													txnCount += 1;
													latency += r.task.finishTime - r.task.timestamp;
													writerate += r.task.write_rate;
													//taskQueue.put(r.task);
												}else{
													piggy++;
													rep_count += r.task.frequency;
													reprate = (float)rep_count / (float)tot_rep;
													rep_txn_count++;
													
													txnSingle.put(r.task.id, true);
													txnCount += 1;
													single_count += 1;
													latency += r.task.finishTime - r.task.timestamp;
													writerate += r.task.write_rate;
													if(repno < r.task.no)
														repno = r.task.no;
												}
											}
											
										}else{
											//if(normal_start == 0)
											//	normal_start = System.currentTimeMillis();
											if(r.task.isSingle)
												single_count++;
											writerate += r.task.write_rate;
											txnCount += 1;
											latency += r.task.finishTime - r.task.timestamp;
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
			
			//System.out.println(rep_txn_count + " repartition transaction are submitted");
			
			float exec_scale = (float)(System.currentTimeMillis() - start) / 60000;
			if(type==1){
				if(normal_end != 0)
					exec_scale = (float)(normal_end - start) / 60000;
			}else if(type==2||type==4){
				exec_scale = (float)(System.currentTimeMillis() - normal_start) / 60000;
			}else if(type == 3 || type == 6){
				if(normal_end != 0 && normal_start == 0)
					exec_scale = (float)(normal_end - start) / 60000;
				else if(normal_end != 0 && normal_start != 0)
					exec_scale = (float)(normal_end - normal_start) / 60000;
				else if(normal_end == 0 && normal_start != 0)
					exec_scale = (float)(System.currentTimeMillis() - normal_start) / 60000;
				else{
					exec_scale = last_exec * ((txnCount+rep_txn_count) / (last_nor+last_rep));
				}
				last_exec = exec_scale;
				last_nor = txnCount;
				last_rep = rep_txn_count;
			}
			float throughput = txnCount / exec_scale;
			
			if(throughput < 0 || throughput > 34000){
				exec_scale = (float)(System.currentTimeMillis() - start) / 60000;
				throughput = txnCount / exec_scale;
			}
			float single_rate = (float) single_count / (float)txnCount;
			
			
			float lat = (float)latency / txnCount;
			
			counter += 1;
			
			//System.out.println("Executed " + (float) counter * (statInterval / 1000.0)+ " seconds");
//			System.out.println("Executed " + (float)(System.currentTimeMillis() - begin) / 1000.0 + " seconds");
			if(counter > warmup_interval){
				//Start repartition task adding
				if(rep_count < tot_rep && tot > 0 && counter < 120){
					if(repTask.isEmpty()){
						
					}else{
						if(type == 0){//Apply all with higher priority than normal txn
							for(Task t:repTask){
								t.priority = t.frequency + 1;
								taskQueue.put(t);
							}
//							System.out.println(repTask.size() + " repartition txns submitted");
							repTask.clear();
						}else if(type == 1){//Apply all with lower priority than normal txn
							for(Task t:repTask){
								t.priority = -1;
								taskQueue.put(t);
							}
							repTask.clear();
						}else if(type == 2){//Apply all with dynamic priority
							LinkedList<Task> submitted = new LinkedList<Task>();
							
							int toRep;
//							if(!taskQueue.isEmpty())
//								toRep = (int) Math.floor(throughput * delta * (statInterval / 60000.0));
//							else{
//								toRep = (int) Math.floor(MaxThroughput * delta * (statInterval / 60000.0));
//							}
//							if(toRep > MaxThroughput * delta * (statInterval / 60000.0))
//								toRep = (int) Math.floor(MaxThroughput * delta * (statInterval / 60000.0));
							
							
							toRep = (int) (Math.floor(((delta - exec_scale / (statInterval / 60000.0)) * max_rep )));
							
							
							
							
							
							
							int i = 0;
							System.out.println(toRep + " repartition txns submitted");
							for(Task t:repTask){
								i++;
								t.priority = t.frequency+1;
								taskQueue.put(t);
								submitted.add(t);
								if(i > toRep)
									break;
							}
							repTask.removeAll(submitted);
							last_txn = throughput;
						}else if(type == 4){
							//Piggyback solution
							LinkedList<Task> submitted = new LinkedList<Task>();
							int toRep;
//							if(!taskQueue.isEmpty())
//								toRep = (int) Math.floor(throughput * delta * (statInterval / 60000.0));
//							else{
//								toRep = (int) Math.floor(MaxThroughput * delta * (statInterval / 60000.0));
//							}
//							if(toRep > MaxThroughput * delta * (statInterval / 60000.0))
//								toRep = (int) Math.floor(MaxThroughput * delta * (statInterval / 60000.0));
							toRep = (int) (Math.floor(((delta - exec_scale / (statInterval / 60000.0)) * max_rep ))) ;
							
							int i = 0;
							if(toRep != 0){
								for(Task t:repTask){
									if(!txnSingle.get(t.id)){
										i++;
										t.priority = t.frequency+1;
										taskQueue.put(t);
									}
									submitted.add(t);


									if(i > toRep)
										break;
								}
								repTask.removeAll(submitted);
							}
						}else if(type == 3){
							//Feedback with AfterAll
							LinkedList<Task> submitted = new LinkedList<Task>();
							int toRep;
							
							toRep = (int) (Math.floor(((delta - 1.0) * (rep_txn_count/2+(float)txnCount/(10.0 * single_rate+2) ))));

							if(toRep > max_rep)
								toRep = max_rep;
							
							if(first_time){
								for(Task t:repTask){
									if(!txnSingle.get(t.id)){
										t.priority = -1;
										taskQueue.put(t);
									}
								}
								first_time = false;
							}else{
								int i = 0;
								if(toRep != 0){
									for(Task t:repTask){
										if(!txnSingle.get(t.id)){
											i++;
											t.priority = 1;
											taskQueue.put(t);
										}
										submitted.add(t);


										if(i > toRep)
											break;
									}
									repTask.removeAll(submitted);
								}
								
							}
							
							
					
						}else if(type == 6){
							//Piggyback solution
							LinkedList<Task> submitted = new LinkedList<Task>();
							int toRep;
							toRep = (int) (Math.floor(((delta - 1.0) * ((float)(txnCount-rep_txn_count)/(12) )))) ;

							if(toRep > max_rep)
								toRep = max_rep;
							
							if(first_time){
								for(Task t:repTask){
									if(!txnSingle.get(t.id)){
										t.priority = -1;
										taskQueue.put(t);
									}
								}
								first_time = false;
							}else{
								int i = 0;
								if(toRep != 0){
									for(Task t:repTask){
										if(!txnSingle.get(t.id)){
											i++;
											t.priority = 1;
											taskQueue.put(t);
										}
										submitted.add(t);


										if(i > toRep)
											break;
									}
									repTask.removeAll(submitted);
								}
								
							}
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
							//System.out.println("Unsupported Algorithm for type = " + type);
							//repTask.clear();
						}
						else{
							System.out.println("Unsupported Algorithm for type = " + type);
						}
					}
				}else{
//					if(!repTask.isEmpty()){
////						System.out.println(repTask.size() + " repartition txns submitted");
//						for(Task t:repTask){
//							t.priority = t.frequency + 1;
//							taskQueue.put(t);
//						}
//						repTask.clear();
//					}
//					if((taskQueue.isEmpty() || finishc != 0) && (tot < 0)){
//						finishc += 1;
//						if(finishc > cooldown_interval)
//							break;
//					}
//					if(rep_count == tot_rep || tot < 0){
//						tCounter++;
//						if(tCounter > cooldown_interval)
//							break;
//					}
				}
			}
			
			last_txn = throughput;
			reprate = (float)rep_count / tot_rep;
			System.out.println("Result" + type + "Workload" + workloadType + "Prog" + preprog + ":" + counter);
			System.out.println("Executed: " + txnCount + " txns, " + single_count + " are single; " + (float) single_count / txnCount + "\t" + writerate / txnCount + "\tFailed: " + failTxnCount);
			System.out.println("Reprate: " + rep_count + "/" + tot_rep + "\t" + reprate + "\t RepTxn: " + rep_txn_count + "\tFailed: " + failRepCount);
			System.out.println("Throughput: " + String.valueOf(txnCount / exec_scale));
			System.out.println("LatencyAvg: " + String.valueOf((double)lat));
			System.out.println("Exec Scale: " + exec_scale);
			System.out.println();
			thro.println(String.valueOf(txnCount / exec_scale) + "\t"
						//+ exec_scale + "\t"
						+ (double)lat + "\t"
						+ txnCount + "\t"
						+ rep_txn_count + "\t"
						+ (float)rep_count / (float)tot_rep + "\t"
						//+ latency + "\t"
						+ failRepCount + "\t"
						+ failTxnCount);
			
			start = System.currentTimeMillis();
			tot_txn += txnCount;
			//act_single += (float) single_count / txnCount;
			
			if(counter > 119)
				break;
			
		}
		
		//thro.println("Tot txn: " + tot_txn + "\t Tot Time: " + (System.currentTimeMillis() - begin));
		//System.out.println("Average throughput: " + tot_txn / (System.currentTimeMillis() - begin));
		thro.close();
		try {
			performance_out.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		//System.out.println("Shutting down the executor");
		pool.shutdown();
		
		try {
			pool.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("All Finish!" + substr);
		
	}
	
	
	
	}
