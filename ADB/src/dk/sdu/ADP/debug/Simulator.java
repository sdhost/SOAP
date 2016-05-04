package dk.sdu.ADP.debug;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.JTA.JTA;
import dk.sdu.ADP.Repartition.RepartitionSearcher;

public class Simulator {
	//Simulation of Repartition Scheduling algorithms
	ADP adp;

	LinkedList<String> history;
	LinkedList<String> workload;
	Map<String,LinkedList<String>> txnDef;
	LinkedList<String> replist = null;
	LinkedList<Integer> load_dist = null;
	
	private Executioner[] e;
	private Repartitioner[] reppro;
	
	private RepartitionSearcher rs = null;
	
	private String workload_file = null;
	private String workload_file_par = null;
	private String workload_file_nopar = null;
	private String addr_lookup = null;
	private String poissdst = null;
	private boolean ispar = false;
	private int warehouse_tot = 0;
	
	private int step = -1;
	
	private int type;
	//type == 1, constant number application, need to set value
	//type == 2, feedback
	//type == 3, Apply all
	//type == 4, Greedy apply
	
	private int amount = -1;
	
	
	public Simulator(){
	}
	
	public void setAmount(int n){
		this.amount = n;
	}
	
	public void Start(String workload, String workload_par, String poissdst, int type){
		if(type==5 || type==6)
			this.StartPiggyback(workload, workload_par, type);
		else{
		
			this.workload_file = workload;
			this.workload_file_par = workload_par;
			this.poissdst = poissdst;
			
			this.type = type;
			if(type == 1 && amount == -1){
				System.out.println("Set the constant amount first before simulating the type 1 experiment");
				return;
			}
			
			try {
				initTest(100, 10, true);
				execTest(100, 10);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void StartPiggyback(String workload, String workload_par, int type){
		this.workload_file = workload;
		this.workload_file_par = workload_par;
		
		this.type = type;
		
		try {
			initTest(30, 1, true);
			execTestPiggyback(30, 1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	private void execTestPiggyback(int numThread, int ReptNO) throws FileNotFoundException, SQLException{
		Map<Integer,Boolean> repfinish = new HashMap<Integer, Boolean>();
		File f = new File(adp.workdir + "ResultType" + type + "Amount" + amount);
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
		
		thro.println("Throughput\tLoadScale\tLatency");
		
		long current = System.currentTimeMillis();
		Thread t[] = new Thread[numThread];
		
		
		step = Integer.valueOf(replist.peekFirst());
		
		for(int i=0; i < numThread; i++){
			String name = "Thread";
			name = name + String.valueOf(i);
			e[i] = new Executioner(name, history, adp.jta, 0.5, step);
			t[i] = new Thread(e[i]);
		}
		e[ReptNO].initTxnPiggyback(new LinkedList<String>(replist.subList(1, replist.size())));
		int txn_count = 0;
		int normal_interval = 20;
		int finish_interval = 20;
		int last_throughput = 0;
		double last_latency = 0;
		int amount = 200;//Initial amount for piggyback solution using feedback
		
		while(true){
			normal_interval--;
			
			System.out.println();
			
			double progress = e[ReptNO].getRepRate();
			
			
			if(progress == 1.0d){
				finish_interval--;
			}
			System.out.println("RepProgress: " + progress);
			for(int i=0;i<numThread;i++){
				if(i!=ReptNO)
					e[i].setRepRate(progress);
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
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
//			for(int i=0;i<numThread;i++){
//				if(!e[i].isStopped())
//					e[i].stopThread();
//			}
			if(normal_interval > 0){
				System.out.println("Repartition begin in: " + normal_interval + " minutes");
			}else if(type == 5 && normal_interval <= 0){
				if(last_throughput <= 0){
					last_throughput = 1000;
				}
				int limit = (int) Math.floor((double)last_throughput * 0.2);
				e[ReptNO].setPiggybackAmount(limit);
			}
			else if(type == 6  && normal_interval <= 0){
				e[ReptNO].setPiggybackAmount(amount);
			}
			
			
			int txn_now = 0;
			long latency_now = 0;
			long max_latency = 0;
			for(int i=0;i<numThread;i++){
				//if(e[i].isStopped()){
					txn_now += e[i].getCount();
					latency_now += e[i].getSumLatency();
					long tmax_latency = e[i].getMaxLatency();
					if(max_latency < tmax_latency)
						max_latency = tmax_latency;
					System.out.print(e[i].getCount() + "\t");
				//}
			}
			last_throughput = txn_now - txn_count;
			double latency = (double)latency_now / (double)last_throughput;
			if(normal_interval <= 0){
				if(latency > last_latency * 1.2){
					amount = amount - 100;
				}else{
					amount = (int) (amount * 2);
				}
				if(amount < 200)
					amount = 200;
			}
			last_latency = latency;
				
			System.out.println();
			System.out.println("Throughput: " + String.valueOf(last_throughput));
			System.out.println("LatencyAvg: " + latency + "\tMax: " + max_latency);
			thro.println(String.valueOf(last_throughput) + "\t" + 1.0 + "\t" + latency + "\t" + max_latency);
			txn_count = txn_now;
			
			
			
			if(finish_interval < 0)
				break;
			
//			if(normal_interval < -3){
//				System.out.println("Stop because of debug setting");
//				return;//for debug use
//			}
		}
		
		thro.close();
		try {
			performance_out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	

	public int initTest(int thread_count, int thread_rep, boolean exec) throws IOException, SQLException{
		int start = 0;
		int par = 5;
		Vector<Properties> inis = JTA.genBTMTest(par);
		 adp = new ADP(inis,"test",5000,1000);
		//End of DB init
		

        System.out.println("Start Load lookupTable");
        File lookup_addr = new File(System.getProperty("user.home") + "/Documents/Result/","lookuptable.ec2.jta.test1.dump");
        ADP.lookupTable.loadLookupTable(lookup_addr.toString());
        System.out.println("End Load lookupTable");
        //End of routing table load
		
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
	        workload = new LinkedList<String>();
			while((line = bin.readLine())!=null){
				workload.add(line);
				
			}
			
			
			
			
			
			bin.close();
			infile.close();
		
			System.out.println("Workload Loaded");
			
			
			txnDef = new HashMap<String, LinkedList<String>>();
			
			processWorkloadTest(workload, exec);//For repartition simulation
			if(exec){
				reppro = new Repartitioner[thread_rep];
				e = new Executioner[thread_count];
			}
			
			System.out.println("Transaction Loaded: count = " + txnDef.size());
		//End of workload load
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
			while((line = bin.readLine())!=null){
				replist.addLast(line);
			}
			
			//Collections.shuffle(replist);
			
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
	
	private void processWorkloadTest(LinkedList<String> workload, boolean partitioned) {
		String flag = null;
		//Set<String> tup = new HashSet<String>();
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
					
					//if(step < 0)
					//	step = queries.size();
					
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
	
	private void execTest(int numThread, int numRept) throws FileNotFoundException, SQLException{
		Map<Integer,Boolean> repfinish = new HashMap<Integer, Boolean>();
		File f = new File(adp.workdir + "ResultType" + type + "Amount" + amount + "T" + numRept);
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
		
		thro.println("Throughput\tLoadScale\tLatency");
		
		long current = System.currentTimeMillis();
		Thread t[] = new Thread[numThread];
		Thread trep[] = new Thread[numRept];
		
		int step_rep = (replist.size() - 1) / numRept;
		int last_total = 0;
		step = Integer.valueOf(replist.peekFirst());
		
		LinkedList<LinkedList<String>> sublists = SpiltList(replist.subList(1, replist.size()), numRept);
		
		for(int i=0;i<numRept;i++){
			reppro[i] = new Repartitioner( sublists.get(i), step_rep, adp.jta, step);
			reppro[i].setStart(0);
			repfinish.put(i, false);
			last_total += step_rep;
			
			
			trep[i] = new Thread(reppro[i]);
			
		}
		for(int i=0; i < numThread; i++){
			String name = "Thread";
			name = name + String.valueOf(i);
			e[i] = new Executioner(name, history, adp.jta, 0.5, step);
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
			if(scale_load >= max)
				scale = 1;
			else
				scale = scale_load / max;
			long start = System.currentTimeMillis();
			long deadlinet = (int)Math.floor(60000 * scale);
			long deadline = 60000 - (int)Math.floor(60000 * scale);
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
			if(type==3 && normal_interval <= 0){
				for(int i=0;i<numRept;i++)
					if(trep[i].getState() == Thread.State.NEW){
						reppro[i].setType(type);
						reppro[i].stop = false;
						trep[i].start();
					}else{
						reppro[i].stop = false;
						synchronized(reppro[i]){
							reppro[i].notify();
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
			}else if(type == 3 && normal_interval <= 0){
				if(deadline > 0){
					try {
						Thread.sleep(deadline);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				for(int i=0;i<numRept;i++){
					if(!reppro[i].stop == false)
						reppro[i].stopThread();
				}
				
			}
			else if(deadline > 0 && type == 1){
				for(int i=0;i<numRept;i++)
					if(trep[i].getState() == Thread.State.NEW){
						reppro[i].setType(type);
						reppro[i].setInterval(amount);
						reppro[i].stop = false;
						trep[i].start();
					}
					else{
						synchronized(reppro[i]){
							reppro[i].stop = false;
							reppro[i].setInterval(amount);
							reppro[i].notify();
						}
					}
				if(deadline > 0){
					try {
						Thread.sleep(deadline);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for(int i=0;i<numRept;i++)
						reppro[i].stopThread();
				}
			}
			//******************************
			//     Code for determinate optimization solution
			else if(deadline > 0 && type == 2){
				System.out.println("Repartition Start");
				for(int i=0;i<numRept;i++){
					if(repfinish.get(i))
						continue;
					reppro[i].stop = false;
					reppro[i].setType(type);
					if(trep[i].getState() == Thread.State.NEW)
						trep[i].start();
					else{
						synchronized(reppro[i]){
							reppro[i].notify();
						}
					}
				}
				try {
					Thread.sleep(deadline);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for(int i=0;i<numRept;i++)
					reppro[i].stopThread();
			}
			//****************************************
			//Code for feedback solution
			else if(deadline > 0 && type == 4){
				for(int i=0;i<numRept;i++)
					if(trep[i].getState() == Thread.State.NEW){
						reppro[i].setType(type);
						reppro[i].stop = false;
						trep[i].start();
					}
					else{
						reppro[i].stop = false;
						synchronized(reppro[i]){
							reppro[i].notify();
						}
					}
				if(deadline > 0){
					try {
						Thread.sleep(deadline);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				for(int i=0;i<numRept;i++)
					reppro[i].stopThread();
				}
			}else{
				System.out.println("Deadline: " + deadline);
			}
			
			
			int txn_now = 0;
			long latency_now = 0;
			long max_latency = 0;
			for(int i=0;i<numThread;i++){
				//if(e[i].isStopped()){
					txn_now += e[i].getCount();
					latency_now += e[i].getSumLatency();
					long tmax_latency = e[i].getMaxLatency();
					if(max_latency < tmax_latency)
						max_latency = tmax_latency;
					System.out.print(e[i].getCount() + "\t");
				//}
			}
			System.out.println();
			System.out.println("Throughput: " + String.valueOf(txn_now - txn_count));
			System.out.println("LatencyAvg: " + String.valueOf((double)latency_now / (double)(txn_now - txn_count)) + "\tMax: " + max_latency);
			thro.println(String.valueOf(txn_now - txn_count) + "\t" + scale + "\t" + (double)latency_now / (double)(txn_now - txn_count) + "\t" + max_latency);
			txn_count = txn_now;
			
			if(finish_interval < 0)
				break;
			
//			if(normal_interval < -3){
//				System.out.println("Stop because of debug setting");
//				return;//for debug use
//			}
		}
		
		thro.close();
		try {
			performance_out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private LinkedList<LinkedList<String>> SpiltList(List<String> list, int num) {
		// Spilt the list average to num sub list, assume the list is sorted
		LinkedList<LinkedList<String>> result = new LinkedList<LinkedList<String>>();
		
		for(int i =0;i<num;i++){
			LinkedList<String> sub = new LinkedList<String>();
			result.add(sub);
		}
		
		for(int i=0; i<list.size();i++){
			int pos = i % num;
			result.get(pos).add(list.get(i));
		}
		
		return result;
			
		
	}

	
	
}
