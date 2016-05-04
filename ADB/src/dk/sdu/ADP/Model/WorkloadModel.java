package dk.sdu.ADP.Model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.util.Pair;
import dk.sdu.ADP.util.PropertySet;
import dk.sdu.ADP.util.TopkQueue;
import dk.sdu.ADP.util.utility;

public class WorkloadModel {
	
	private HashMap<String,Integer> transactionFrequency;//T_p:(TxnID, Count)
	private TopkQueue<Pair>  top_transactions;//Topk of T_p
	private HashMap<String,Map<String, PropertySet<String>>> transactionPartitions;
	//T_{info}: (TxnID,(TupleID, Partition))
	private HashMap<String,Integer> datanodeLoad;
	//Overall sum L_p: (Partition, Load)
	
	private HashMap<String,Set<String>> tupleWrite;
	private HashMap<String,Set<String>> tupleRead;
	//Inverted index for tuples and transactions
	
	//private HashMap<String,Integer> transactionLoad;
	
	private LinkedList<HashMap<String,List<String>>> LoadList;
	//L_p: ((Partition,(TxnID)))
	private HashMap<String,List<String>> currentLoad;
	private HashMap<String,Integer> currentIntervalFreq;//T_p in current Interval
	private HashMap<String,Map<String, PropertySet<String>>> currentIntervalPart;// T_{info} in current Interval
	private LinkedList<String> newTran;//Current L_{history}: (TxnID)
	private LinkedList<LinkedList<String>> history;//Overall L_{history}: ((TxnID))
	
	
	private int interval;//Size for each time interval
	private int count = 0;//Counter for processed txn, reset everytime a interval is full
	private int datanodes;//Number of partitions, |N|
	
	private int intervalCount;
	private int top_size; //The count of topk that will be returned as search candidate
	
	
	
	private CostModel cmodel;//Used for calculation of the design cost
	
	

	public WorkloadModel(int predictionIntervalCount, int interval, int top_size, int nodecount) {
		
		transactionFrequency = new HashMap<String,Integer>();
		top_transactions = new TopkQueue<Pair>(top_size);
		transactionPartitions = new HashMap<String,Map<String, PropertySet<String>>>();
		//transactionLoad = new HashMap<String,Integer>();
		datanodeLoad = new HashMap<String,Integer>();
		currentLoad = new HashMap<String,List<String>>();
		for(int i=1;i<=nodecount;i++){
			datanodeLoad.put(String.valueOf(i), 0);
			currentLoad.put(String.valueOf(i), new LinkedList<String>());
		}
		this.datanodes = nodecount;
		tupleRead = new HashMap<String,Set<String>>();
		tupleWrite = new HashMap<String,Set<String>>();
		
		this.intervalCount = predictionIntervalCount;
		this.interval = interval;
		this.top_size = top_size;
		
		history = new LinkedList<LinkedList<String>>();
		newTran = new LinkedList<String>();
		
		LoadList = new LinkedList<HashMap<String,List<String>>>();
		
		
		currentIntervalFreq = new HashMap<String,Integer>();
		currentIntervalPart = new HashMap<String,Map<String,PropertySet<String>>>();
		newTran = new LinkedList<String>();
		
		cmodel = new CostModel(top_size, 1, 5); //Test alpha and beta use 1
		
		this.count = 0;
		
	}
	
	public void insertRecord(Map<String,PropertySet<String>> tuples, String txni){
		//String txni for debugging
		//New transaction recorded in the system
		boolean full = this.intervalInsert(tuples, txni);
		if(full)
			this.insertRecord();
		if(this.transactionFrequency.size() != this.transactionPartitions.size())
			System.out.println();
	}
	
	private void insertRecord() {
		//an new interval is full, update the T_p and L_p and history
		//Update history, transactionFrequency, top_transactions, transactionPartitions,
		//datanodeLoad, LoadList
		if(this.currentIntervalFreq.size() != this.currentIntervalPart.size())
			System.out.println();
		LinkedList<String> oldhis = null;
		HashMap<String,List<String>> oldload = null;
		
		//synchronized(this.newTran){
			// A new interval is full, update the sliding window and models
			if(this.history.size() >= this.intervalCount){
				oldhis = this.history.poll();
				oldload = this.LoadList.poll();
			}
			this.history.add(newTran);//Pass value by inference
			this.transactionPartitions.putAll(this.currentIntervalPart);//Pass value by inference
			//UpdateIidx(this.currentIntervalPart);

			this.LoadList.offer(this.currentLoad);// Pass value by inference
			
			//Insert new value to the datanodeLoad and transactionFrequency
			for(Entry<String,Integer> e: this.currentIntervalFreq.entrySet()){
				int freq;
				if(this.transactionFrequency.containsKey(e.getKey()))
					freq = this.transactionFrequency.get(e.getKey());
				else
					freq = 0;
				this.transactionFrequency.put(e.getKey(), e.getValue() + freq);// Pass by value
			}
			
			
 			if(oldhis != null){
				for(String key: oldhis){
					int freq = 0;
					if(this.transactionFrequency.containsKey(key))
						freq = this.transactionFrequency.get(key);
					else
						continue;
					if(freq == 1){
						//this.transactionFrequency.remove(key);
						RemoveTransaction(key);
					}else{
						this.transactionFrequency.put(key, freq - 1);
					}
				}//Delete frequency statistic of the deleted history
				
				for(Entry<String,List<String>> e: oldload.entrySet()){
					int load = this.datanodeLoad.get(e.getKey());
					if(load < e.getValue().size()){
						System.out.println("Error in workload!!" + e.getKey() + ":" + load + "-" + e.getValue().size());
						
						load = e.getValue().size();
					}
					this.datanodeLoad.put(e.getKey(), load - e.getValue().size());
				}//Delete workload on each partition of the deleted history
			}
			
			
			
			for(Entry<String,List<String>> e: this.currentLoad.entrySet()){
				int load = this.datanodeLoad.get(e.getKey());
				this.datanodeLoad.put(e.getKey(), e.getValue().size() + load);
			}
			
			Map<String, Integer> sorted = utility.sortByValue(this.transactionFrequency);
			int i = 0;
			this.top_transactions.clear();
			for(Entry<String, Integer> e: sorted.entrySet()){
				i += 1;
				this.top_transactions.put(new Pair(e.getKey(), e.getValue()));
				if(i>=this.top_size)
					break;
			}
		
			//Reset all the temporary list
			this.newTran = new LinkedList<String>();
			this.currentLoad = new HashMap<String,List<String>>();
			for(int j=1;j<=datanodes;j++){
				currentLoad.put(String.valueOf(j), new LinkedList<String>());
			}
			this.currentIntervalPart = new HashMap<String,Map<String,PropertySet<String>>>();
			this.currentIntervalFreq.clear();
			this.count = 0;
		//}
		
		
		
	}

	private void RemoveTransaction(String key) {
		this.transactionFrequency.remove(key);
		this.transactionPartitions.remove(key);
//		Map<String, PropertySet<String>> tuples = this.transactionPartitions.get(key);
//		if(tuples == null){
//			System.out.println("No partition information for txnKey: " + key);
//			return;
//		}
//			
//		for(Entry<String,PropertySet<String>> tuple: tuples.entrySet()){
//			if(this.tupleRead.get(tuple.getKey()) != null)
//				this.tupleRead.get(tuple.getKey()).remove(key);
//			if(this.tupleWrite.get(tuple.getKey()) != null)
//				this.tupleWrite.get(tuple.getKey()).remove(key);
//		}
		
	}

	private void UpdateIidx(
			HashMap<String, Map<String, PropertySet<String>>> txnpart) {
		for(Entry<String,Map<String,PropertySet<String>>> txn:txnpart.entrySet())
			for(Entry<String,PropertySet<String>> tuple:txn.getValue().entrySet()){
				if(tuple.getValue().isWrite())
					if(this.tupleWrite.containsKey(tuple.getKey()))
						this.tupleWrite.get(tuple.getKey()).add(txn.getKey());
					else{
						Set<String> newset = new HashSet<String>();
						newset.add(txn.getKey());
						this.tupleWrite.put(tuple.getKey(), newset);
					}
				else
					if(this.tupleRead.containsKey(tuple.getKey()))
						this.tupleRead.get(tuple.getKey()).add(txn.getKey());
					else{
						Set<String> newset = new HashSet<String>();
						newset.add(txn.getKey());
						this.tupleRead.put(tuple.getKey(), newset);
					}
						
			}
		
	}
	
	

	private boolean intervalInsert(Map<String, PropertySet<String>> tuples, String txni) {
		//String txni for debugging
		// Insert new transaction record to the interval statistics
		//Update newTran,currentLoad,currentIntervalFreq,currentIntervalPart
		if(tuples.isEmpty())
			return false;
		
		count += 1;
		
		//Calculate the key and load
		//int load;
		//Set<String> tup = new HashSet<String>();
		String transactionKey = txni;
		Set<String> partitions = new HashSet<String>();
					
		for(Iterator it = tuples.entrySet().iterator();it.hasNext();){
			Entry tuple = (Entry)it.next();
			//tup.add((String) tuple.getKey());
			for(String par:(PropertySet<String>)tuple.getValue()){
				if(!partitions.contains(par)){
					partitions.add(par);
				}
			}
		}		
		//load = tuples.size();// Alternatives
		//load = 1;//Simplification
		int freq = 0;		
		//transactionKey = ADP.lookupTable.getTxnKey(tup);
		//DEBUG
		//if(!transactionKey.contentEquals(txni)){
		//	System.out.println("Error, incorrect original transaction key!");
		//}
		//END DEBUG
		
		//End key and load
		
		//Update currentLoad
		for(String s: partitions){
			currentLoad.get(s).add(transactionKey);
		}
		//End currentLoad
		
		// Store Timeline info
		this.newTran.offer(transactionKey);

		//End timeline
		
		//Update transaction frequency
		if(this.currentIntervalFreq.containsKey(transactionKey)){
			freq = this.currentIntervalFreq.get(transactionKey) + 1;
			this.currentIntervalFreq.put(transactionKey,freq);
		}else{
			this.currentIntervalFreq.put(transactionKey,1);
			freq = 1;
		}
		//End update frequency
		
		//Update partitions
		this.currentIntervalPart.put(transactionKey, tuples);
		//End update partitions and workload
		
		if(count >= this.interval){
			//A new interval is full
			return true;
		}
		
		
		return false;
	}


	public Set<String> getInsertNodes() {
		
		Set<String> result = new HashSet<String>();
		//Version 1: Always Insert tuple to the data node that has the minimum workload
		String min_par = this.getMinPar();
		result.add(min_par);
		
		//Version 2: Return all the data nodes except the one with maximum workload
		// Unfinished
		
		//Version 3: Return all the data nodes
//		for(int i=1;i<=this.datanodes;i++)
//			result.add(String.valueOf(i));
		return result;
	}
	
	public String getMinPar(){
		Set<String> result = new HashSet<String>();
		String min_par = null;
		int min_load = Integer.MAX_VALUE;
		
		for(Iterator it = this.datanodeLoad.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, Integer> e = (Map.Entry<String, Integer>) it.next();
			if(e.getValue() < min_load){
				min_par = e.getKey();
				min_load = e.getValue();
			}
		}
		return min_par;
	}
	
	
	public void saveModel(File addr) throws IOException{
		if(!addr.exists())
			addr.createNewFile();
		else{
			addr.delete();
			addr.createNewFile();
		}
		
		FileOutputStream out = new FileOutputStream(addr,false);
		PrintStream pout = new PrintStream(out);
		
		for(Entry<String, Map<String, PropertySet<String>>> e: this.transactionPartitions.entrySet()){
			pout.println(e.getKey());//Transaction Key, one line
			for(Entry<String,PropertySet<String>> tuple: e.getValue().entrySet()){
				String line = tuple.getKey();
				if(tuple.getValue().isWrite())
					line += "\tW";
				else
					line += "\tR";
				for(String par:tuple.getValue()){
					line += "\t" + par;
				}
				pout.println(line);//Tuple partition and type, one line
			}
			pout.println("END");//Transaction Separator, one line
		}//Predefined transactions stored
		
		pout.println("HISTORY");
		for(LinkedList<String> h_i:history){
			for(String txn:h_i){
				pout.println(txn);//Transaction history, one line per txn
			}
			pout.println("INTERVAL");//Interval separator, one line
		}
		
		
		System.out.println("Model saved at: " + addr);
		
		pout.close();
		out.close();
		
	}
	
	public void loadModel(File addr) throws IOException{
		
		if(!addr.exists()){
			System.out.println("File not exist!");
			return;
		}
		
		FileReader in = new FileReader(addr);
		BufferedReader bin = new BufferedReader(in);
		String line = null, txnKey = null;
		int step = 0;
		Map<String, Map<String,PropertySet<String>>> tpar = new HashMap<String, Map<String, PropertySet<String>>>();
		//LinkedList<String> his = new LinkedList<String>();
		Map<String,PropertySet<String>> tuples = new HashMap<String, PropertySet<String>>();
		while((line = bin.readLine()) != null){
			if(line.equalsIgnoreCase("END") && step == 0){
				tpar.put(txnKey, tuples);
				tuples = new HashMap<String, PropertySet<String>>();
				txnKey = null;

			}else if(line.equalsIgnoreCase("HISTORY") && step == 0){
				step = 1;
			}else if(line.equalsIgnoreCase("INTERVAL") && step == 1){
				continue;//Manually divide the interval if use time as reference
			}else if(txnKey == null && step == 0){
				txnKey = line;
			}else if(step == 0){
				String[] elem = line.split("\t");
				String tk = elem[0];
				String wt = elem[1];
				boolean write;
				if(wt.equalsIgnoreCase("W"))
					write = true;
				else
					write = false;
				PropertySet<String> par = new PropertySet<String>(write);
				for(int i = 2; i < elem.length; i++){
					par.add(elem[i]);
				}
				tuples.put(tk, par);
			}else if(step == 1){
				this.insertRecord(tpar.get(line), txnKey);//Remember to remove the txnKey
			}
		}
		
		
		System.out.println("Model loaded from: " + addr);
		
		bin.close();
		in.close();
	}

	public void insertLoad(String par) {
		// add 1 load to the partition par
		
		int load = this.datanodeLoad.get(par);
		this.datanodeLoad.put(par, load+1);
		
	}
	
	
	public double getDistCost(){
		if(this.notFull())
			return -Double.MAX_VALUE;
		return CostModel.DistCost(transactionFrequency, transactionPartitions, this.history.size() * this.interval, datanodes);
	}
	
	public double getSkew(){
		if(this.notFull())
			return -Double.MAX_VALUE;
		return CostModel.Skew(LoadList, datanodes);
	}
	
	public double getDesignCost(){
		if(this.notFull())
			return -Double.MAX_VALUE;
			
		return CostModel.DesignCost(transactionFrequency, transactionPartitions, LoadList, this.history.size() * this.interval, datanodes);
	}
	
	public double getDesignCost(HashMap<String,Map<String, PropertySet<String>>> newdesign, LinkedList<HashMap<String,List<String>>> newLoad){
		return CostModel.DesignCost(transactionFrequency, newdesign, newLoad, this.history.size() * this.interval, datanodes);
	}
	
	public LinkedList<HashMap<String,List<String>>> getLoad(){
		return this.LoadList;
	}
	
	public HashMap<String,Map<String, PropertySet<String>>> getDesign(){
		return this.transactionPartitions;
	}
	
	public List<String> getTopk(){
		return this.top_transactions.gettopk(this.top_size, true);
	}
	
	public int getTxnFreq(String txn){
		return this.top_transactions.getFreq(txn);
	}
	
	public int ParCount(String txnKey){
		if(!this.transactionPartitions.containsKey(txnKey))
			return -1;
		Set<String> par = new HashSet<String>();
		for(Entry<String,PropertySet<String>> e : this.transactionPartitions.get(txnKey).entrySet()){
			par.addAll(e.getValue());
		}
		return par.size();
	}
	
	public PropertySet<String> getPartitions(String txnKey){
		boolean current = false;
		if(!this.currentIntervalPart.containsKey(txnKey)){
			if(!this.transactionPartitions.containsKey(txnKey))
				return null;
		}else{
			current = true;
		}
			
		PropertySet<String> par = new PropertySet<String>();
		if(!current)
			for(Entry<String,PropertySet<String>> e : this.transactionPartitions.get(txnKey).entrySet()){
				par.addAll(e.getValue());
				if(e.getValue().isWrite())
					par.setWrite();
			}
		else
			for(Entry<String,PropertySet<String>> e : this.currentIntervalPart.get(txnKey).entrySet()){
				par.addAll(e.getValue());
				if(e.getValue().isWrite())
					par.setWrite();
			}
		return par;
	}
	
	public Set<String> getTuples(String txnKey){
		if(!this.transactionPartitions.containsKey(txnKey))
			return null;
		Set<String> tuple = new HashSet<String>();
		for(Entry<String,PropertySet<String>> e : this.transactionPartitions.get(txnKey).entrySet()){
			tuple.add(e.getKey());
		}
		return tuple;
	}
	
	public PropertySet<String> getTuplePartition(String txnKey, String tupleKey){
		return this.transactionPartitions.get(txnKey).get(tupleKey);
	}

	public Set<String> getAllNode(String par) {
		Set<String> allnode = new HashSet<String>();
		int tar = Integer.valueOf(par);
		for(int i = 1; i <= this.datanodes; i++){
			if(i==tar)
				continue;
			allnode.add(String.valueOf(i));
		}
		return allnode;
	}

	public Set<String> getAllNode(PropertySet<String> par) {
		if(par.isWrite()){
			Set<String> allnode = new HashSet<String>();
			for(int i = 1; i <= this.datanodes; i++){
				allnode.add(String.valueOf(i));
			}
			allnode.removeAll(par);
			return allnode;
		}else
			return this.getAllNode(par.iterator().next());
	}

	public Set<String> getReadTransaction(String tuplei) {
		return this.tupleRead.get(tuplei);
	}

	public Set<String> getWriteTransaction(String tuplei) {
		return this.tupleWrite.get(tuplei);
	}

	public int getLoad(String par) {
		return this.datanodeLoad.get(par);
	}
	
	public void updateDesign(HashMap<String, Map<String, PropertySet<String>>> tDesign,
			LinkedList<HashMap<String, List<String>>> tLoad){
		this.transactionPartitions = tDesign;
		this.LoadList = tLoad;
		
		for(int i=1;i<=this.datanodes;i++)
			this.datanodeLoad.put(String.valueOf(i), 0);
		for(HashMap<String, List<String>> load:tLoad)
			for(Entry<String,List<String>> e:load.entrySet()){
				int lc = e.getValue().size();
				lc += this.datanodeLoad.get(e.getKey());
				this.datanodeLoad.put(e.getKey(), lc);
			}
	}
	
	public PropertySet<String> getTxnTuplePar(String txnKey, String tupleKey){
		if(this.currentIntervalPart.containsKey(txnKey)){
			return this.currentIntervalPart.get(txnKey).get(tupleKey);
		}else if(this.transactionPartitions.containsKey(txnKey)){
			return this.transactionPartitions.get(txnKey).get(tupleKey);
		}else
			return null;
	}
	
	public Map<String, PropertySet<String>> getRouting(String txnKey){
		if(this.currentIntervalPart.containsKey(txnKey))
			return this.currentIntervalPart.get(txnKey);
		else if(this.transactionPartitions.containsKey(txnKey))
			return this.transactionPartitions.get(txnKey);
		else
			return null;
	}

	public boolean notFull() {
		
		return (this.history.size() < this.intervalCount);
	}

}
