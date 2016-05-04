package dk.sdu.ADP.Model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Set;

import dk.sdu.ADP.util.PropertySet;

public class CostModel {
		
		static int k;
		static private int cpar, dtxn;
		static int parcount;
		static double alpha, beta;
		
		public CostModel(int k, double alpha, double beta){
			this.k = k;
			this.alpha = alpha;
			this.beta = beta;
		}
		
		public static double DesignCost(Map<String, Integer> t_p,
										Map<String, Map<String, PropertySet<String>>> par,
										LinkedList<HashMap<String, List<String>>> loadList, 
										int txn, int nodecount){
			double result = 0, dist, skew;
			//
			dist = DistCost(t_p,par,txn,nodecount);
			skew = Skew(loadList,nodecount);
			
			result = (alpha * dist + beta * skew) / (alpha + beta);
			
			return result;
		}
	
		static public double DistCost(Map<String, Integer> t_p, Map<String, Map<String, PropertySet<String>>> par, int txn, int nodecount){
			double result = 0.0;
			
			calc_parameter(t_p,par,txn);
			
			result = (double)cpar / (double)(nodecount * txn) * (1.0 + (double)dtxn / txn);
			//parcount = nodecount;
//			System.out.println("Calculating DistCost: DC=" + result
//						+ ", Dtxn=" + dtxn
//						+ ", txn=" + txn
//						+ ", cPar=" + cpar);
			return result;
			
			
		}

		private static void calc_parameter(Map<String, Integer> t_p,
				Map<String, Map<String, PropertySet<String>>> par, int txn) {
			dtxn = 0;
			cpar = 0;
			int i = 0;
			for(Entry<String,Integer> e : t_p.entrySet()){
				i++;
				//if(i>k && k != -1)
				//	break; // Control for top-k
				Set<String> partition = new HashSet<String>();
				for(Entry<String, PropertySet<String>> tuple: par.get(e.getKey()).entrySet()){
					partition.addAll(tuple.getValue());
				}
				if(partition.size() > 1){
					cpar += partition.size();
					dtxn += e.getValue();
				}
			}
			
			
			
		}
		
		public static double Skew(LinkedList<HashMap<String, List<String>>> loadList, int nodecount){
			double result = 0;
			parcount = nodecount;
			int intervalcount = loadList.size();
			for(HashMap<String, List<String>> m: loadList){
				result += s(m);
			}
			int intervalCount = loadList.size();
			
			//System.out.println("Skew = " + result / (double)intervalCount);
			return result / (double)intervalCount;//Debug, ignore the skew
			//return 0;
		}


		private static double s(HashMap<String, List<String>> m) {
			double result = 0;
			int c = 0;
			double h_max = Math.log(parcount) / Math.log(2);
			for(Entry<String, List<String>> e:m.entrySet()){
				c += e.getValue().size();
			}
			for(Entry<String, List<String>> e:m.entrySet()){
				int ci = e.getValue().size();
				if(ci == 0)
					continue;//0*log(0) = 0
				else
					result += (double)ci/c * (Math.log((double)ci/c) / Math.log(2));
			}
			result = h_max - result;
			return result / h_max;
		}

		
		
}
