package dk.sdu.ADP.Model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.util.Pair;
import dk.sdu.ADP.util.utility;

public class LookUpTable {
	
	
	private HashMap<String, Set<String>> table;
	public HashMap<String, ArrayList<String>> tuplePK = null; //Initialed outside this class
	public HashMap<String, String> tupleTable = null;//Initialed outside this class
	
	private HashMap<String, Set<String>> table_insearch;
	
	private HashMap<String, Set<Pair>> deleted;
	
	//private HashMap<String, Set<String>> txnTable;
	
	private HashMap<String, TuplePosition> raw;
	
	public HashMap<String,ArrayList<String>> keylist;
	
	private HashSet<String> datanodes;
	
	public void init(int nodecount){
		//txnTable = new HashMap<String,Set<String>>();
		deleted = new HashMap<String,Set<Pair>>();
		table = new HashMap<String,Set<String>>();
		table_insearch = new HashMap<String,Set<String>>();
		keylist = new HashMap<String,ArrayList<String>>();
		datanodes = new HashSet<String>();
		raw = new HashMap<String,TuplePosition>();
		for(int i = 1; i <= nodecount; i++)
			this.datanodes.add(String.valueOf(i));
	}
	
	public void loadKeylist(String table_name, ArrayList<String> list){
		
		keylist.put(table_name, list);
	}
	public List<String> getKeyList(String table_name){
		List<String> result = null;
		result = keylist.get(table_name);

		return result;
	}
	
	public String getKey(Map<String,String> value, String table) throws SQLException{
		String table_name = table.toLowerCase();
		String key = table_name;
		if(keylist.containsKey(table_name)){
			ArrayList<String> names = new ArrayList<String>();
			names.addAll(keylist.get(table_name));
			for(Iterator it = names.iterator();it.hasNext();){
				String col_key = (String) it.next();
				if(value.containsKey(col_key)){
					key += col_key + value.get(col_key);
				}
				else{
					//throw new SQLException("Insufficient Key value for table: " + table_name);
					return null;
				}
			}
		}else{
			throw new SQLException("Table " + table_name + " Not Defined in LookUpTable");
		}
		
		
		
		return utility.md5(key);
	}
	
	public void insert(Map<String,String> value, String table_name, String partition){
		String key;
		try {
			key = getKey(value,table_name);
			if(tupleTable != null){
				tupleTable.put(key, table_name);
			}
			if(tuplePK != null){
				ArrayList<String> l = new ArrayList<String>();
				for(String col : keylist.get(table_name)){
					l.add(value.get(col));
				}
				tuplePK.put(key, l);
			}
			//Map<String, String> cols = new HashMap<String,String>();
			//cols.putAll(value);
			//if(cols.containsKey(ADP.partition_col))
			//	cols.remove(ADP.partition_col);
			//if(!this.raw.containsKey(key))
			//	this.updateRaw(key, cols, table_name);
			this.insert(key, partition);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		
	}
	public String getTableName(String key){
		return this.raw.get(key).table;
	}
	
	public String getSQL(String key, String par, String type){
		return this.raw.get(key).getSQL(par, type);
	}
	public String getSQLJTA(String key, String type){
		return this.raw.get(key).getSQLJTA(type);
	}
	
	public TuplePosition getRaw(String key){
		return this.raw.get(key);
	}
	
	public void insert(String key, String partition){
		
//		if(!this.raw.containsKey(key)){
//			System.err.println("Insert tuple without the tuple position!");
//			return;
//		}
		
		if(table.containsKey(key)){
			if(!table.get(key).contains(partition))
				table.get(key).add(partition);
			else{
				
				//
				//Index already in lookup table
				//
			}
		}else{
			Set<String> newlist = new HashSet<String>();
			newlist.add(partition);
			table.put(key, newlist);
		}
		
		if(this.deleted.containsKey(key))
			this.deleted.remove(key);
		
	}
	
	public Set<String> get(String key){
		if(key == null)
			return null;
		else
			return this.get(key,-1);//Version 1 get
	}
	
	public Set<String> get(String key, int tranCount){
		Set<String> result = this.table.get(key);
		Set<Pair> ex = this.deleted.get(key);
		if(ex==null)
			return result;
		else{
			for(Pair p:ex){
				if(tranCount > p.getValue()){
					result.remove(p.getKey());
				}
			}
			return result;
		}
	}
	
//	public Set<String> getTxn(String key){
//		//key for transaction key
//		return txnTable.get(key);
//	}
//	public void insertTxn(String key, Set<String> partitions){
//		this.txnTable.put(key, partitions);
//	}
	
	public String getTxnKey(Set<String> tuples){
		String transactionKey = "";
		Set<String> partitions = new HashSet<String>();
					
		for(Iterator it = tuples.iterator();it.hasNext();){
			String tuple = (String) it.next();
			transactionKey += tuple;
		}			
		transactionKey = utility.md5(transactionKey);
		return transactionKey;
	}
	
	public Set<String> get(Map<String,String> value, String table_name) throws SQLException{
		
		String key = getKey(value,table_name);
		return this.get(key);

	}
	public Set<String> get(Map<String,String> value, String table_name, int tranCount) throws SQLException{
		
		String key = getKey(value,table_name);
		return this.get(key,tranCount);

	}
	
	public boolean delete(Map<String,String> value, String table_name, String partition){
		
		try {
			String key = this.getKey(value, table_name);
			this.delete(key, partition, 0);//Version 1 delete
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		
		
		return true;
	}
	
	public void delete(String key, String partition, int tranCount){
		
		//Version 1: Delete the record in table
		/*
		if(table.containsKey(key)){
			table.get(key).remove(partition);
			if(table.get(key).isEmpty())
				this.raw.remove(key);
		}
		*/
		
		//Version 2: Use a deleted list, use the transaction Count as timestamp
		if(table.containsKey(key)){
			if(deleted.containsKey(key)){
				deleted.get(key).add(new Pair(partition,tranCount));
			}else{
				Set<Pair> s = new HashSet<Pair>();
				s.add(new Pair(partition,tranCount));
				deleted.put(key, s);
			}
		}
		
	}

	public Set<String> getAllNodes() {
		
		return this.datanodes;
	}
	
	public void loadLookupTable(String addr) throws IOException{
		//TODO: Load predefined lookuptable from a file
		File infile = new File(addr);
		if(!infile.exists())
			System.out.println("File url incorrect: " + addr);
		
		FileReader in = new FileReader(infile);
		BufferedReader bin = new BufferedReader(in);
		
		bin.readLine(); //First line is for timestamp, ignore
		
		String line;
		line = bin.readLine();
		if(line == null){
			bin.close();
			in.close();
			return;
		}
		Integer size = Integer.valueOf(line); //LookupTable size
		for(int i = 0; i < size; i++){
			line = bin.readLine();
			String subline = line.substring(0, line.indexOf("|"));
			String[] line_value = subline.split("\t");
			String key = line_value[0];
			Set<String> values = new HashSet<String>();
			for(int j = 1; j < line_value.length;j++){			
				values.add(line_value[j]);
			}
			this.table.put(key, values);
			
//			if(tuplePK == null || tupleTable == null){
//				continue;
//			}
			if(tuplePK == null)
				continue;
			subline = line.substring(line.indexOf("|") + 3);
			
			line_value = subline.split("\t");
			String table_name = line_value[0];
			ArrayList<String> l = new ArrayList<String>();
			for(int j = 1; j < line_value.length;j++){			
				l.add(line_value[j]);
			}
			
			//tupleTable.put(key, table_name);
			tuplePK.put(key, l);
			
		}
		
		line = bin.readLine();
		size = Integer.valueOf(line); //Raw key to tuple size
		for(int i = 0; i < size; i++){
			line = bin.readLine();
			String[] line_value = line.split("\t");
			String key = line_value[0];
			TuplePosition tp = new TuplePosition(line_value);
			this.raw.put(key, tp);
		}
		
		line = bin.readLine();
		size = Integer.valueOf(line); //keylist size
		
		for(int i = 0; i < size; i++){
			line = bin.readLine();
			String[] line_value = line.split("\t");
			String key = line_value[0];
			ArrayList<String> values = new ArrayList<String>();
			for(int j = 1; j < line_value.length;j++){			
				values.add(line_value[j]);
			}
			this.keylist.put(key, values);
		}
		
		line = bin.readLine();
		size = Integer.valueOf(line); //deleted size
		
		for(int i = 0; i < size; i++){
			line = bin.readLine();
			String[] line_value = line.split("\t");
			String key = line_value[0];
			Set<Pair> parset = new HashSet<Pair>();
			for(int j = 1; j < line_value.length; j=j+2){
				Pair p = new Pair(line_value[j], Integer.valueOf(line_value[j+1]));
				parset.add(p);
			}
			this.deleted.put(key, parset);
		}
			
			
		//while(!(line = bin.readLine()).isEmpty()){
		//	this.datanodes.add(line);
		//}
		
		System.out.println("LookupTable Loaded: " + addr);
		System.out.println(String.valueOf(this.table.size()) + " tuples loaded");
		
		bin.close();
		in.close();
	}
	
	public void saveLookuptable(String addr) throws IOException{
		//Store current lookuptable to disk
		File outfile = new File(addr);
		if(!outfile.exists())
			outfile.createNewFile();
		
		FileOutputStream out = new FileOutputStream(outfile,false);
		PrintStream pout = new PrintStream(out);
		pout.println("LookupTable: " + new Date().getTime());
		
		pout.println(this.table.size());
		for(Iterator it = this.table.entrySet().iterator(); it.hasNext();){
			Entry e = (Entry) it.next();
			String newline = e.getKey() + "\t";
			for(Iterator kit = ((Set<String>)e.getValue()).iterator(); kit.hasNext();){
				String par = (String) kit.next();
				newline += par + "\t";
			}
			
			newline += "| \t";
			
			
			
			if(tupleTable != null && tuplePK != null){
				newline += tupleTable.get(e.getKey()) + "\t";
				for(String pk:tuplePK.get(e.getKey())){
					newline += pk + "\t";
				}
				
			}
			pout.println(newline);
		}
		
		pout.println(this.raw.size());
		for(Iterator it = this.raw.entrySet().iterator(); it.hasNext();){
			Entry<String,TuplePosition> e = (Entry<String, TuplePosition>) it.next();
			String newline = e.getKey() + "\t";
			newline += e.getValue().toString();
			
			pout.println(newline);
		}
		
		pout.println(this.keylist.size());
		for(Iterator it = this.keylist.entrySet().iterator(); it.hasNext();){
			Entry e = (Entry) it.next();
			String newline = e.getKey() + "\t";
			for(Iterator kit = ((ArrayList<String>)e.getValue()).iterator(); kit.hasNext();){
				String par = (String) kit.next();
				newline += par + "\t";
			}
			pout.println(newline);
		}
		
		pout.println(this.deleted.size());
		for(Iterator it = this.deleted.entrySet().iterator(); it.hasNext();){
			Entry<String,Set<Pair>> e = (Entry<String,Set<Pair>>)it.next();
			String newline = e.getKey()+"\t";
			for(Iterator<Pair> lit = e.getValue().iterator();lit.hasNext();){
				Pair p = lit.next();
				newline += p.getKey() + "\t" + p.getValue();
			}
			pout.println(newline);
		}
		
		
		//for(Iterator it = this.datanodes.iterator();it.hasNext();){
		//	pout.println((String)it.next());
		//}
		
		System.out.println("LookupTable Saved: " + addr);
		
		pout.close();
		out.close();
		
	}
	
	public void updateRaw(String key, Map<String,String> cols, String table){
		TuplePosition tp = new TuplePosition(cols,table);
		this.raw.put(key, tp);
	}
	
	public void insertSearch(String key, Set<String> par){
		this.table_insearch.put(key, par);
	}
	
	public Set<String> getInsearch(String key){
		if(this.table_insearch.containsKey(key))
			return this.table_insearch.get(key);
		else if(this.table.containsKey(key)){
			return this.get(key);
		}else{
			return null;
		}
	}
	
	public void resetSearch(){
		this.table_insearch.clear();
	}

	public HashMap<String, Vector<String>> getParMenu() {
		HashMap<String, Vector<String>> menu = new HashMap<String, Vector<String>>();
		
		for(Entry<String, Set<String>> e: this.table.entrySet()){
			for(String par: e.getValue()){
				if(menu.get(par) == null){
					Vector<String> v = new Vector<String>();
					v.add(e.getKey());
					menu.put(par, v);
				}else{
					menu.get(par).add(e.getKey());
				}
			}
		}
		
		return menu;
	}
}
