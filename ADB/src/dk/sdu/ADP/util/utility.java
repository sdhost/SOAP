package dk.sdu.ADP.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class utility {
	private static MessageDigest md;
	
	public static void init(String digestMethod){
		
		try {
			md = MessageDigest.getInstance(digestMethod);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		
	}
	
	public static String md5(String base){
		byte[] bytes = md.digest(base.getBytes());
		
		BigInteger md5v = new BigInteger(1,bytes);
		base = String.format("%0" + (bytes.length << 1) + "X", md5v);
		return base;
	}
	
	public static <K, V extends Comparable<? super V>> Map<K, V> 
    sortByValue( Map<K, V> map )
    {
	    List<Map.Entry<K, V>> list =
	        new LinkedList<Map.Entry<K, V>>( map.entrySet() );
	    Collections.sort( list, new Comparator<Map.Entry<K, V>>()
	    {
	        public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
	        {
	            return (o2.getValue()).compareTo( o1.getValue() );
	        }
	    } );
	
	    Map<K, V> result = new LinkedHashMap<K, V>();
	    for (Map.Entry<K, V> entry : list)
	    {
	        result.put( entry.getKey(), entry.getValue() );
	    }
	    return result;
	}
	
	public static HashMap<String,Map<String, PropertySet<String>>> copyDesign(HashMap<String,Map<String, PropertySet<String>>> design){
		HashMap<String,Map<String, PropertySet<String>>> copy = new HashMap<String,Map<String, PropertySet<String>>>();
		for(Iterator<Entry<String,Map<String, PropertySet<String>>>> it = design.entrySet().iterator(); it.hasNext();){
			Entry<String,Map<String, PropertySet<String>>> e = it.next();
			Map<String, PropertySet<String>> tuples = new HashMap<String,PropertySet<String>>();
			String txnKey = e.getKey();
			for(Iterator<Entry<String, PropertySet<String>>> nit = e.getValue().entrySet().iterator(); nit.hasNext();){
				Entry<String,PropertySet<String>> ne = nit.next();
				PropertySet<String> par = new PropertySet<String>(ne.getValue().isWrite());
				String tupleKey = ne.getKey();
				for(String s:ne.getValue()){
					String ns = s;
					par.add(ns);
				}
				tuples.put(tupleKey, par);
				
			}
			copy.put(txnKey, tuples);
			
		}
		
		return copy;
		
	}
	
	public static LinkedList<HashMap<String,List<String>>> copyLoad(LinkedList<HashMap<String,List<String>>> load){
		LinkedList<HashMap<String,List<String>>> copy = new LinkedList<HashMap<String,List<String>>>();
		
		for(Iterator<HashMap<String,List<String>>> it = load.iterator(); it.hasNext();){
			HashMap<String,List<String>> intLoad = new HashMap<String,List<String>>();
			HashMap<String,List<String>> e = it.next();
			for(Iterator<Entry<String, List<String>>> nit = e.entrySet().iterator(); nit.hasNext();){
				Entry<String, List<String>> ne = nit.next();
				String parKey = ne.getKey();
				List<String> txns = new ArrayList<String>();
				for(String s: ne.getValue()){
					String ns = s;
					txns.add(ns);
				}
				intLoad.put(parKey, txns);
			}
			copy.add(intLoad);
		}
		
		
		return copy;
		
	}
	
}
