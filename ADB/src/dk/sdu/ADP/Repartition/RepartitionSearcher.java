package dk.sdu.ADP.Repartition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

public class RepartitionSearcher implements Runnable {
	
	private ADP adp;
	private long timeInterval;
	public Set<Pair> change = null;
	public boolean hasNew = false;
	public HashMap<String,Map<String, PropertySet<String>>> last_design = null;
	public LinkedList<HashMap<String,List<String>>> last_load = null;
	public double last_cost = Double.MAX_VALUE;
	
	public int transactionCount = 0;
	
	public RepartitionSearcher(ADP adp, int timeInterval){
		//timeInterval with unit of second
		this.adp = adp;
		this.timeInterval = timeInterval * 1000;
		this.change = new HashSet<Pair>();
		this.hasNew = false;
	}
	
	public RepartitionSearcher(ADP adp){
		//Debug constructer without thread
		this.adp = adp;
		//this.timeInterval = timeInterval * 1000;
		this.change = new HashSet<Pair>();
		this.hasNew = false;
	}
	
	public int getSingle(){
		if(hasNew){
			int single = 0;
			for(Entry<String, Map<String, PropertySet<String>>> e:last_design.entrySet()){
				Set<String> cover = new HashSet<String>();
				for(PropertySet<String> s:e.getValue().values()){
					if(cover.isEmpty()){
						cover.addAll(s);
					}else{
						cover.retainAll(s);
						if(cover.isEmpty())
							break;
					}
				}
				if(!cover.isEmpty())
					single++;
			}
			
			return single;
			
		}else{
			return -1;
		}
	}
	
	@Override
	public void run() {
		while(true){
			
			//Search(1);
			
			
			
			//Wait for next search
			try {
				Thread.sleep(this.timeInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

//	public void Search(int tranCount) {
//		
//		if(ADP.model.notFull())
//			return;
//		
//		this.transactionCount = tranCount;
//		
//		boolean hasNewDesign = false;
//		List<String> topk = ADP.model.getTopk();
//		HashMap<String,Map<String, PropertySet<String>>> design = ADP.model.getDesign();
//		LinkedList<HashMap<String,List<String>>> load = ADP.model.getLoad();
//		double lastDesignCost = Double.MAX_VALUE;
////		if(last_design != null){
////			lastDesignCost = ADP.model.getDesignCost(last_design, last_load);
////		}//always search from current design
//		double cDesignCost = ADP.model.getDesignCost();
//		
//		double lDesignCost;
//		if(lastDesignCost < cDesignCost){
//			design = last_design;
//			load = last_load;
//			lDesignCost = lastDesignCost;
//		}else{
//			change.clear();
//			ADP.lookupTable.resetSearch();
//			lDesignCost = cDesignCost;
//			//Choose the current design, delete the last one
//		}
//		
//		
//		
//		for(String txnKey : topk){
//			boolean single = false;
//			Set<String> partitionSet = ADP.model.getPartitions(txnKey);
//			if(partitionSet == null){
//				System.out.println("Empty transaction in system with Key: " + txnKey);
//				continue;
//			}
//			if(partitionSet.size() == 1)
//				single = true;
//			String min_par = ADP.model.getMinPar();
//			partitionSet.add(min_par);//get N_set
//			Set<String> tupleSet = ADP.model.getTuples(txnKey);// get Tuple_set
//			HashMap<String,Map<String, PropertySet<String>>> bestDesign = null;
//			LinkedList<HashMap<String,List<String>>> bestLoad = null;
//			double maxDec = 0;
//			
//			Set<Pair> tmpChange = null;
//			
//			if(single){
//				
//				String par = partitionSet.iterator().next();//Get the partiton of this transaction
//				Set<String> all_par = ADP.model.getAllNode(par);
//				for(String newPar: all_par){
//					HashMap<String,Map<String, PropertySet<String>>> tDesign;
//					tDesign = utility.copyDesign(design);
//					LinkedList<HashMap<String,List<String>>> tLoad;
//					tLoad = utility.copyLoad(load);
//					Set<Pair> tChange = new HashSet<Pair>();
//					//ADP.lookupTable.resetSearch();
//					for(String tuplei: tupleSet){
//						if(adp.lookupTable.get(tuplei).contains(newPar))
//							tChange.addAll(DeleteTuple(tuplei,par, tDesign, tLoad, false));
//						else
//							tChange.addAll(MoveTuple(tuplei, par, newPar, tDesign, tLoad));
//					}
//					double tDesignCost = ADP.model.getDesignCost(tDesign, tLoad);
//					if(lDesignCost - tDesignCost > maxDec){
//						maxDec = lDesignCost - tDesignCost;
//						bestDesign = tDesign;
//						bestLoad = tLoad;
//						tmpChange = tChange;
//					}
//				}
//				if(bestDesign != null){
//					hasNewDesign = true;
//					design = bestDesign;
//					load = bestLoad;
//					change.addAll(tmpChange);
//					bestDesign = null;
//					maxDec = 0;
//				}
//			}else{
//				for(String tuplei:tupleSet){
//					PropertySet<String> par = ADP.model.getTuplePartition(txnKey, tuplei);
//					Set<String> all_par = ADP.model.getAllNode(par);
//					String maxPar = par.iterator().next();
//					int maxLoad = ADP.model.getLoad(maxPar);
//					
//					//Not used in final version
//					//Temporary Solution for Tuple Delete
//					//Only consider the delete for topk transaction, that has the popular write frequency
//					//Different to the data nodes decision procedure which consider more one cold tuples
//					if(par.isWrite() && par.size() > 1){
//						for(String delPar: par){
//							//ADP.lookupTable.resetSearch();
//							HashMap<String,Map<String, PropertySet<String>>> tDesign = new HashMap<String,Map<String, PropertySet<String>>>();
//							tDesign = utility.copyDesign(design);
//							LinkedList<HashMap<String,List<String>>> tLoad = new LinkedList<HashMap<String,List<String>>>();
//							tLoad = utility.copyLoad(load);
//							Set<Pair> tChange = new HashSet<Pair>();
//							
//							tChange.addAll(DeleteTuple(tuplei, delPar, tDesign, tLoad, false));
//							double tDesignCost = ADP.model.getDesignCost(tDesign, tLoad);
//							
//							if(ADP.model.getLoad(delPar) > maxLoad){
//								maxLoad = ADP.model.getLoad(delPar);
//								maxPar = delPar;
//							}
//							
//							if(lDesignCost - tDesignCost > maxDec){
//								maxDec = lDesignCost - tDesignCost;
//								bestDesign = tDesign;
//								bestLoad = tLoad;
//								tmpChange = tChange;
//							}
//						}
//						if(bestDesign != null){
//							hasNewDesign = true;
//							design = bestDesign;
//							load = bestLoad;
//							change.addAll(tmpChange);
//							bestDesign = null;
//							maxDec = 0;
//						}
//					}
//					//End Temporary Solution
//					
//					for(String newPar: all_par){
//						
//						HashMap<String,Map<String, PropertySet<String>>> tDesignM = new HashMap<String,Map<String, PropertySet<String>>>();
//						tDesignM = utility.copyDesign(design);
//						LinkedList<HashMap<String,List<String>>> tLoadM = new LinkedList<HashMap<String,List<String>>>();
//						tLoadM = utility.copyLoad(load);
//						Set<Pair> tChangeM = new HashSet<Pair>();
//						//ADP.lookupTable.resetSearch();
//						if(adp.lookupTable.get(tuplei).contains(newPar))
//							tChangeM.addAll(DeleteTuple(tuplei, maxPar, tDesignM, tLoadM, false));
//						else
//							tChangeM.addAll(MoveTuple(tuplei, maxPar, newPar, tDesignM, tLoadM));
//						double tDesignCostM = ADP.model.getDesignCost(tDesignM, tLoadM);
//						
//						
//						HashMap<String,Map<String, PropertySet<String>>> tDesignI = new HashMap<String,Map<String, PropertySet<String>>>();
//						tDesignI = utility.copyDesign(design);
//						LinkedList<HashMap<String,List<String>>> tLoadI = new LinkedList<HashMap<String,List<String>>>();
//						tLoadI = utility.copyLoad(load);
//						Set<Pair> tChangeI = new HashSet<Pair>();
//						//ADP.lookupTable.resetSearch();
//						
//						if(!adp.lookupTable.get(tuplei).contains(newPar))
//							tChangeI.addAll(InsertTuple(tuplei, newPar, tDesignI, tLoadI));
//						double tDesignCostI = ADP.model.getDesignCost(tDesignI, tLoadI);
//						
//						double tDesignCost;
//						HashMap<String,Map<String, PropertySet<String>>> tDesign;
//						LinkedList<HashMap<String,List<String>>> tLoad;
//						Set<Pair> tChange;
//						if(tDesignCostM < tDesignCostI){
//							tDesign = tDesignM;
//							tLoad = tLoadM;
//							tChange = tChangeM;
//							tDesignCost = tDesignCostM;
//						}else{
//							tDesign = tDesignI;
//							tLoad = tLoadI;
//							tChange = tChangeI;
//							tDesignCost = tDesignCostI;
//						}
//						
//						if(lDesignCost - tDesignCost > maxDec){
//							maxDec = lDesignCost - tDesignCost;
//							bestDesign = tDesign;
//							bestLoad = tLoad;
//							tmpChange = tChange;
//						}
//					}
//					if(bestDesign != null){
//						hasNewDesign = true;
//						design = bestDesign;
//						load = bestLoad;
//						change.addAll(tmpChange);
//						bestDesign = null;
//						maxDec = 0;
//					}
//				}
//			}
//			
//		}
//		
//		if(hasNewDesign){
//			this.last_design = design;
//			this.last_load = load;
//			this.last_cost = ADP.model.getDesignCost(design, load);
//			//synchronized(this.change){
//			this.hasNew = true;
//			//this.change.notify();//Call for decision making thread
//			//}
//		}
//		
//	}
//	
	public void NewSearch(int txnStamp){
		//Output: last_design, last_load, last_cost, change
		
		this.transactionCount = txnStamp;
		
		boolean hasNewDesign = false;
		List<String> topk = ADP.model.getTopk();//Used for local search 
		HashMap<String,Map<String, PropertySet<String>>> design;
		LinkedList<HashMap<String,List<String>>> load;
		
		double cDesignCost = ADP.model.getDesignCost();
		if(cDesignCost < 0)
			return;
		
		double lDesignCost;
		if(last_cost < cDesignCost){
			design = last_design;
			load = last_load;
			lDesignCost = last_cost;
		}else{
			design = ADP.model.getDesign();
			load = ADP.model.getLoad();
			change.clear();
			//ADP.lookupTable.resetSearch();
			lDesignCost = cDesignCost;
			//Choose the current design, delete the last one
		}
		
		for(String txn: topk){
			boolean single = false;
			Set<String> partitionSet = getPartitions(txn, design);
			if(partitionSet == null){
				System.out.println("Empty transaction in system with Key: " + txn);
				continue;
			}
			if(partitionSet.size() == 1)
				single = true;
			
			Set<String> tupleSet = getTuples(txn, design);// get Tuple_set
			Map<String,Set<String>> iidx_tuple = new HashMap<String,Set<String>>();// get each partition's tuple, used for search
			for(String tuple:tupleSet){
				PropertySet<String> pars = getTuplePartition(txn, tuple, design);
				if(!partitionSet.containsAll(pars)){
					System.out.println("Error, inconsistent among model");
					pars.retainAll(partitionSet);
				}
				if(pars.write){		
					for(String par:pars){		
						if(iidx_tuple.containsKey(par)){
							iidx_tuple.get(par).add(tuple);
						}else{
							Set<String> s = new HashSet<String>();
							s.add(tuple);
							iidx_tuple.put(par, s);
						}
					}
				}//End if par.write
				else{
					assert pars.size() == 1 : "Read a tuple with more than one partition!";
					String par = pars.iterator().next();
					if(iidx_tuple.containsKey(par)){
						iidx_tuple.get(par).add(tuple);
					}else{
						Set<String> s = new HashSet<String>();
						s.add(tuple);
						iidx_tuple.put(par, s);
					}
				}//End else par.write
			}
			assert iidx_tuple != null : "No tuple visited for transaction";
			HashMap<String,Map<String, PropertySet<String>>> bestDesign = design;
			LinkedList<HashMap<String,List<String>>> bestLoad = load;
			double bestCost = lDesignCost;
			
			Set<Pair> tmpChange = null;
			
			if(single){
				String min_par = getMinPar(load);
				partitionSet.add(min_par);//Add partition with min_load to single partitioned txn's for relaxation
			}//End if single
			
			for(String par:partitionSet){
				HashMap<String,Map<String, PropertySet<String>>> tdesign = utility.copyDesign(design);;
				LinkedList<HashMap<String,List<String>>> tload = utility.copyLoad(load);
				assert tdesign != design;
				assert tload != load;
				assert iidx_tuple != null;
				Set<Pair> tChange = SearchBranch(tdesign,tload,txn,par,iidx_tuple, partitionSet, single);
				if(tChange == null)
					continue;
				double tDesignCost = ADP.model.getDesignCost(tdesign, tload);
				if(tDesignCost < bestCost ){
					bestDesign = tdesign;
					bestLoad = tload;
					tmpChange = tChange;
					bestCost = tDesignCost;
				}
				
			}//End for partitionSet	
			
			if(bestCost < lDesignCost){
				design = bestDesign;
				load = bestLoad;
				lDesignCost = bestCost;
				//System.out.format("%04f", lDesignCost);
				change.addAll(tmpChange);
				tmpChange.clear();//Tested, will not affect the 'change' set
				hasNewDesign = true;
			}
			
		}//End for topk
		if(hasNewDesign){
			//System.out.println();
			//System.out.println(lDesignCost);
			this.last_cost = lDesignCost;
			this.last_design = design;
			this.last_load = load;
			this.hasNew = true;
		}
	}

	private String getMinPar(LinkedList<HashMap<String, List<String>>> load) {
		Map<String, Integer> node_load = new HashMap<String,Integer>();
		for(HashMap<String, List<String>> loadi:load)
			for(Entry<String,List<String>> e:loadi.entrySet()){
				if(node_load.containsKey(e.getKey())){
					node_load.put(e.getKey(), node_load.get(e.getKey()) + e.getValue().size());
				}else{
					node_load.put(e.getKey(), e.getValue().size());
				}
			}
		int min_load = Integer.MAX_VALUE;
		String min_par = null;
		for(Entry<String,Integer> e: node_load.entrySet()){
			if(min_load > e.getValue()){
				min_load = e.getValue();
				min_par = e.getKey();
			}
				
		}
		return min_par;
	}

	private Set<String> getTuples(String txn,
			HashMap<String, Map<String, PropertySet<String>>> design) {
		return design.get(txn).keySet();
	}

	private PropertySet<String> getTuplePartition(String txn, String tuple, HashMap<String, Map<String, PropertySet<String>>> design) {
		return design.get(txn).get(tuple);
	}

	private Set<String> getPartitions(String txn, HashMap<String, Map<String, PropertySet<String>>> design) {
		Set<String> pars = new HashSet<String>();
		for(Entry<String,PropertySet<String>> tp:design.get(txn).entrySet()){
			pars.addAll(tp.getValue());
		}
		return pars;
	}

	private Set<Pair> SearchBranch(
			HashMap<String, Map<String, PropertySet<String>>> design,
			LinkedList<HashMap<String, List<String>>> load, String txn,
			String par, Map<String, Set<String>> iidx_tuple, Set<String> partitionSet, boolean single) {
		// Choose a search strategy to find a different partition design.
		Set<Pair> result1 = new HashSet<Pair>();
		HashMap<String, Map<String, PropertySet<String>>> new_design1 = utility.copyDesign(design);
		LinkedList<HashMap<String, List<String>>> new_load1 = utility.copyLoad(load);
		result1 = MovingAllToOne(new_design1, new_load1, txn, par, iidx_tuple);
		double cost1 = ADP.model.getDesignCost(new_design1, new_load1);
		
		if(single)
			return result1;
		
		Set<Pair> result2 = new HashSet<Pair>();
		HashMap<String, Map<String, PropertySet<String>>> new_design2 = utility.copyDesign(design);
		LinkedList<HashMap<String, List<String>>> new_load2 = utility.copyLoad(load);
		result2 = MovingOneToOther(new_design2, new_load2, txn, par, iidx_tuple, partitionSet);
		double cost2 = ADP.model.getDesignCost(new_design2, new_load2);
		
		if(cost1 <= cost2 && result1 != null){
			design.clear();
			design.putAll(new_design1);
			load.clear();
			load.addAll(new_load1);
			return result1;
		}else if(cost2 < cost1 && result2 != null){
			design.clear();
			load.clear();
			design.putAll(new_design2);
			load.addAll(new_load2);
			return result2;
		}else{
			return null;
		}
		
		
		
	}

	private Set<Pair> MovingOneToOther(
			HashMap<String, Map<String, PropertySet<String>>> design,
			LinkedList<HashMap<String, List<String>>> load, String txn,
			String par, Map<String, Set<String>> iidx_tuple, Set<String> partitionSet) {
		Set<Pair> result = new HashSet<Pair>();
		
		if(!iidx_tuple.containsKey(par)){
			System.out.println(iidx_tuple.size());
			Map<String,PropertySet<String>> tp = design.get(txn);
			System.out.println(design.get(txn).size());
			return result;
		}
		
		for(String tuple:iidx_tuple.get(par)){
			//Get par can be null
			HashMap<String, Map<String, PropertySet<String>>> bestd = null;
			LinkedList<HashMap<String, List<String>>> bestl = null;
			double bestc = 0;
			Set<Pair> trs = null;
			
			for(String spar:partitionSet){
				if(spar.equals(par))
					continue;
				Set<Pair> result1 = new HashSet<Pair>();
				HashMap<String, Map<String, PropertySet<String>>> new_design1 = utility.copyDesign(design);
				LinkedList<HashMap<String, List<String>>> new_load1 = utility.copyLoad(load);
				result1 = InsertTuple(tuple, spar, new_design1, new_load1);
				double cost1 = Double.MAX_VALUE;
				if(result1 != null)
					cost1 = ADP.model.getDesignCost(new_design1, new_load1);
				
				Set<Pair> result2 = new HashSet<Pair>();
				HashMap<String, Map<String, PropertySet<String>>> new_design2 = utility.copyDesign(design);
				LinkedList<HashMap<String, List<String>>> new_load2 = utility.copyLoad(load);
				result2 = MoveTuple(tuple, par, spar, new_design2, new_load2);
				double cost2 = Double.MAX_VALUE;
				if(result2 != null)
					cost2 = ADP.model.getDesignCost(new_design2, new_load2);
				
				if(bestd == null || cost1 < bestc || cost2 < bestc){
					if(cost2 < cost1){
						trs = result2;
						bestd = new_design2;
						bestl = new_load2;
						bestc = cost2;
					}else{
						trs = result1;
						bestd = new_design1;
						bestl = new_load1;
						bestc = cost1;
					}
				}//End if bestd == null
				
				
			}//End For partitionSet
			assert bestd != null : "Partition set with only one candidate?";
			design.clear();
			load.clear();
			design.putAll(bestd);
			load.addAll(bestl);
			result.addAll(trs);
		}//End for iidx_tuple.get(par)
		return result;
	}

	private Set<Pair> MovingAllToOne(
			HashMap<String, Map<String, PropertySet<String>>> design,
			LinkedList<HashMap<String, List<String>>> load, String txn,
			String par, Map<String, Set<String>> iidx_tuple) {
		Set<Pair> result = new HashSet<Pair>();
		for(Iterator<Entry<String,Set<String>>> it = iidx_tuple.entrySet().iterator(); it.hasNext();){
			Entry<String,Set<String>> e = it.next();
			if(e.getKey().equalsIgnoreCase(par))
				continue;
			for(String tuple:e.getValue()){
				//Try insert a new replica to the target partition
				Set<Pair> result1 = new HashSet<Pair>();
				HashMap<String, Map<String, PropertySet<String>>> new_design1 = utility.copyDesign(design);
				LinkedList<HashMap<String, List<String>>> new_load1 = utility.copyLoad(load);
				result1 = InsertTuple(tuple, par, new_design1, new_load1);
				double cost1 = ADP.model.getDesignCost(new_design1, new_load1);
				
				//Try delete the original one, that is move the tuple replica to target partition
				Set<Pair> result2 = new HashSet<Pair>();
				HashMap<String, Map<String, PropertySet<String>>> new_design2 = utility.copyDesign(design);
				LinkedList<HashMap<String, List<String>>> new_load2 = utility.copyLoad(load);
				result2 = MoveTuple(tuple, e.getKey(), par, new_design2, new_load2);
				double cost2 = ADP.model.getDesignCost(new_design2, new_load2);
				
				
				if(cost1 > cost2){
					result.addAll(result2);
					design.clear();
					load.clear();
					design.putAll(new_design2);
					load.addAll(new_load2);
				}else{
					result.addAll(result1);
					design.clear();
					load.clear();
					design.putAll(new_design1);
					load.addAll(new_load1);
				}		
			}//End for e.getValue()
		}//End for iidx_tuple
		return result;
	}

//	private Set<Pair> DeleteTuple(
//			String tuplei,
//			String delPar,
//			HashMap<String, Map<String, PropertySet<String>>> tDesign,
//			LinkedList<HashMap<String, List<String>>> tLoad,
//			boolean moving) {
//		//TODO: Old version, not used in new search
//		Set<Pair> result = new HashSet<Pair>();
//		if(ADP.lookupTable.get(tuplei) == null || !ADP.lookupTable.get(tuplei).contains(delPar) || (!moving && ADP.lookupTable.get(tuplei).size() < 2))
//			return result;
//		result.add(new Pair(tuplei,-transactionCount));
//		
//		Set<String> alt = new HashSet<String>();
//		for(String s:ADP.lookupTable.getInsearch(tuplei)){
//			String ns = s;
//			alt.add(ns);
//		}
//		
//		alt.remove(delPar);
//		
//		final Set<String> rtxns = ADP.model.getReadTransaction(tuplei);
//		final Set<String> wtxns = ADP.model.getWriteTransaction(tuplei);
//		
//		if(rtxns != null)
//			for(String txn:rtxns){
//				if(tDesign.get(txn).get(tuplei).contains(delPar)){
//					Set<String> partitions = new HashSet<String>();
//					for(Entry<String,PropertySet<String>> e:tDesign.get(txn).entrySet()){
//						partitions.addAll(e.getValue());
//					}
//					partitions.retainAll(alt);
//					int minLoad = Integer.MAX_VALUE;
//					String minPar = null;
//					for(String par:partitions){
//						if(ADP.model.getLoad(par) < minLoad){
//							minLoad = ADP.model.getLoad(par);
//							minPar = par;
//						}
//					}
//					if(minPar == null)
//						for(String par:alt){
//							if(ADP.model.getLoad(par) < minLoad){
//								minLoad = ADP.model.getLoad(par);
//								minPar = par;
//							}
//						}
//					
//					tDesign.get(txn).get(tuplei).remove(delPar);
//					tDesign.get(txn).get(tuplei).add(minPar);
//					
//					for(HashMap<String,List<String>> load: tLoad){
//						if(load.containsKey(delPar) && load.get(delPar).contains(txn)){
//							load.get(delPar).remove(txn);
//							load.get(minPar).add(txn);
//						}
//					}
//					
//				}
//			}
//		if(wtxns != null)
//			for(String txn:wtxns){
//				tDesign.get(txn).get(tuplei).remove(delPar);
//				for(HashMap<String,List<String>> load: tLoad){
//					load.get(delPar).remove(txn);
//				}
//			}
//		
//		
//		return result;
//	}

	private Set<Pair> InsertTuple(String tuplei,
			String newPar,
			HashMap<String, Map<String, PropertySet<String>>> tDesignI,
			LinkedList<HashMap<String, List<String>>> tLoad) {
		
		Set<Pair> result = new HashSet<Pair>();
		int gain = 0;
		
		Set<String> rtxns = new HashSet<String>();
		Set<String> wtxns = new HashSet<String>();
		getTxn(tuplei, tDesignI, rtxns, wtxns);
		
		if(rtxns!=null)
			for(String txn:rtxns){
				if(tDesignI.get(txn).get(tuplei).contains(newPar)){
					//System.out.println("Cannot occur in Insert Tuple function");
					continue;
				}
				String oldPar = String.valueOf(tDesignI.get(txn).get(tuplei).iterator().next());
				Set<String> partitions = new HashSet<String>();
				int c_old = 0, c_new = 0;
				for(Entry<String,PropertySet<String>> e:tDesignI.get(txn).entrySet()){
					partitions.addAll(e.getValue());
					if(e.getValue().contains(oldPar))
						c_old++;
					if(e.getValue().contains(newPar))
						c_new++;
				}
				
				if(c_new>0){
					
					if(oldPar != newPar){
						tDesignI.get(txn).get(tuplei).remove(oldPar);
						tDesignI.get(txn).get(tuplei).add(newPar);
						if(c_old == 1){
							gain += adp.model.getTxnFreq(txn);
							for(HashMap<String,List<String>> load: tLoad){
								while(load.get(oldPar).contains(txn)){
									load.get(oldPar).remove(txn);
									load.get(newPar).add(txn);
								}
							}
						}
					}
				}			
			}
		if(wtxns != null){
			for(String txn:wtxns){
				int c = 0;
				for(Entry<String,PropertySet<String>> e:tDesignI.get(txn).entrySet()){
					if(e.getValue().contains(newPar))
						c++;
				}
				String oldPar = tDesignI.get(txn).get(tuplei).iterator().next();
				tDesignI.get(txn).get(tuplei).add(newPar);
				if(c == 0){
					gain -= adp.model.getTxnFreq(txn);
					for(HashMap<String,List<String>> load: tLoad){
						int k = Collections.frequency(load.get(oldPar), txn);
						for(int i=0;i<k;i++)
							load.get(newPar).add(txn);//TODO: check If the load list is restored after each search
					}
				}
			}
		}
		result.add(new Pair(tuplei + "\t" + gain, Integer.valueOf(newPar)));
		
		return result;
	}

	private void getTxn(String tuple,
			HashMap<String, Map<String, PropertySet<String>>> design,
			Set<String> rtxn, Set<String> wtxn) {
		// TODO Generate rtxn and wtxn list for tuple
		//Version1: search all the txns
		for(Iterator<Entry<String,Map<String, PropertySet<String>>>> it = design.entrySet().iterator(); it.hasNext();){
			Entry<String,Map<String, PropertySet<String>>> e = it.next();
			if(e.getValue().containsKey(tuple)){
				if(e.getValue().get(tuple).isWrite())
					wtxn.add(e.getKey());
				else
					rtxn.add(e.getKey());
			}
		}
		
	}

	private Set<Pair> MoveTuple(String tuplei, String oldPar, String newPar,
			HashMap<String, Map<String, PropertySet<String>>> tDesign,
			LinkedList<HashMap<String, List<String>>> tLoad) {
		Set<Pair> result = new HashSet<Pair>();
//		result.addAll(this.InsertTuple(tuplei, newPar, tDesign, tLoad));
//		result.addAll(this.DeleteTuple(tuplei, oldPar, tDesign, tLoad, true));
		
		int gain = 0;
		
		Set<String> rtxns = new HashSet<String>();
		Set<String> wtxns = new HashSet<String>();
		getTxn(tuplei, tDesign, rtxns, wtxns);
		
		if(rtxns!=null){
			for(String txn:rtxns){
				if(tDesign.get(txn).get(tuplei).contains(newPar)){
					//System.out.println("Cannot occur in Insert Tuple function");
					continue;
				}
				Set<String> partitions = new HashSet<String>();
				int c_old = 0, c_new = 0, c_ori = 0;
				String oPar = String.valueOf(tDesign.get(txn).get(tuplei).iterator().next());
				for(Entry<String,PropertySet<String>> e:tDesign.get(txn).entrySet()){
					partitions.addAll(e.getValue());
					if(e.getValue().contains(newPar))
						c_new++;
					if(e.getValue().contains(oPar))
						c_ori++;
					if(e.getValue().contains(oldPar))
						c_old++;
				}
					
				if(oPar != newPar){
					tDesign.get(txn).get(tuplei).remove(oPar);
					tDesign.get(txn).get(tuplei).add(newPar);
					if(c_ori == 1){
						gain += adp.model.getTxnFreq(txn);
						for(HashMap<String,List<String>> load: tLoad){
							while(load.get(oPar).contains(txn)){
								load.get(oPar).remove(txn);
								load.get(newPar).add(txn);
							}
						}
					}
				}
				
				if(tDesign.get(txn).get(tuplei).contains(oldPar)){
					tDesign.get(txn).get(tuplei).remove(oldPar);
					if(tDesign.get(txn).get(tuplei).isEmpty())
						tDesign.get(txn).get(tuplei).add(newPar);
					if(c_old == 1){
						gain += adp.model.getTxnFreq(txn);
						for(HashMap<String,List<String>> load: tLoad){
							while(load.get(oldPar).contains(txn)){
								load.get(oldPar).remove(txn);
								load.get(newPar).add(txn);
							}
						}
					}
				}
			}
		}
		if(wtxns != null){
			for(String txn:wtxns){
				int c_new = 0, c_old = 0;
				for(Entry<String,PropertySet<String>> e:tDesign.get(txn).entrySet()){
					if(e.getValue().contains(newPar))
						c_new++;
					if(e.getValue().contains(oldPar))
						c_old++;
				}
				tDesign.get(txn).get(tuplei).add(newPar);
				tDesign.get(txn).get(tuplei).remove(oldPar);
				if(c_new == 0){
					gain -= adp.model.getTxnFreq(txn);
					for(HashMap<String,List<String>> load: tLoad){
						int k = Collections.frequency(load.get(oldPar), txn);
						for(int i=0;i<k;i++)
							load.get(newPar).add(txn);
					}
				}
				if(c_old == 1){
					gain += adp.model.getTxnFreq(txn);
					for(HashMap<String,List<String>> load: tLoad){
						while(load.get(oldPar).contains(txn))
							load.get(oldPar).remove(txn);
					}
				}
			}
		}
		
		result.add(new Pair(tuplei + "\t" + String.valueOf(gain / 2), Integer.valueOf(newPar)));
		result.add(new Pair(tuplei + "\t" + String.valueOf(gain - gain / 2),-transactionCount));
		
		return result;
	}
	
	public void reset_design() {
		//Reset design and load list after applied
		if(this.last_design != null){
			this.last_design = null;
		}
		
		if(this.last_load != null){
			this.last_load = null;
		}
		
		this.last_cost = Double.MAX_VALUE;
	}

}
