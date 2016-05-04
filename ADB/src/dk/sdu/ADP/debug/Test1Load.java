package dk.sdu.ADP.debug;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.JTA.JTA;
import dk.sdu.ADP.util.Pair;

public class Test1Load {
	
	private ADP adp = null;
	private FileOutputStream out;
	private PrintStream pout;
	private int tranc, queryc;
	private double wrate;
	private int par;
//	private HashMap<String, Vector<String>> parMenu;
//	private HashMap<String, Integer> parSize;
	private int single;
	private List<Integer> Zipf;
	private int step;
	
	public Test1Load(double write, int step){
		this.wrate = write;
		this.step = step;
	}
	
	public Test1Load(double write){
		this.wrate = write;
		this.step = 5;
	}
	
	public void GenZipf(String file){
		
		FileReader infile = null;
        BufferedReader bin = null;
        
		File zipf_file = new File(file);
		if(zipf_file.exists()){
			try {
				infile = new FileReader(zipf_file);
				bin = new BufferedReader(infile);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}	
		}else{
			System.out.println("Error Reading Zipf Distribution File: " + file);
			return;
		}
		
		Zipf = new LinkedList<Integer>();
		String line;
		try {
			while((line = bin.readLine()) != null){
				Zipf.add((int)Math.floor(Float.valueOf(line)));
			}
			bin.close();
			infile.close();
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		
		
		this.par = 5;
		  
//		Vector<Properties> inis = JTA.genBTMTest(par);
//		adp = new ADP(inis,"test",1000000,1000);
//		
//		adp.lookupTable.tuplePK = new HashMap<String, ArrayList<String>>();
//		
//		System.out.println("Start Load lookupTable");
//        File lookup_addr = new File(System.getProperty("user.home") + "/Documents/Result/","lookuptable.ec2.jta.test1.dump");
//        
//        try {
//			adp.lookupTable.loadLookupTable(lookup_addr.toString());
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//        System.out.println("End Load lookupTable");

        try {
        	String name = "Test1WorkloadZipf";
        	File f = new File(System.getProperty("user.home") + "/Documents/Result/" + name + "@" + String.valueOf(System.currentTimeMillis() / 10000));
        	
        	if(!f.exists())
        		f.createNewFile();
        	
			this.out = new FileOutputStream((f), false);
			pout = new PrintStream(out);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        System.out.println("Finished Initialization!");
        genZipf();
        pout.close();
        try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        System.out.println("Finished Generation!");
		
	}
	


	public void GenRandom(){
		this.par = 5;
		  
//		Vector<Properties> inis = JTA.genBTMTest(par);
//		adp = new ADP(inis,"test",1000000,1000);
//		
//		adp.lookupTable.tuplePK = new HashMap<String, ArrayList<String>>();
//		//adp.lookupTable.tupleTable = new HashMap<String, String>();
//		
//		System.out.println("Start Load lookupTable");
//        File lookup_addr = new File(System.getProperty("user.home") + "/Documents/Result/","lookuptable.ec2.jta.test1.dump");
//        
//        try {
//			adp.lookupTable.loadLookupTable(lookup_addr.toString());
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//        System.out.println("End Load lookupTable");

        
        try {
        	String name = "Test1Workload";
        	File f = new File(System.getProperty("user.home") + "/Documents/Result/" + name + "@" + String.valueOf(System.currentTimeMillis() / 10000));
        	
        	if(!f.exists())
        		f.createNewFile();
        	
			this.out = new FileOutputStream((f), false);
			pout = new PrintStream(out);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        this.tranc = 150000 / step;
        
        System.out.println("Finished Initialization!");
        gen();
        pout.close();
        try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        System.out.println("Finished Generation!");
	}
	
	private void gen() {
		
		Map<Integer, Integer> idmap = new HashMap<Integer, Integer>();
		Map<Integer, Integer> freq = new HashMap<Integer, Integer>();
		
		single = 0;
		int id = 0;
		Random rn = new Random();
		rn.setSeed(System.currentTimeMillis());

		for(int i=0;i<this.tranc;i++){
			pout.println("BEGIN");
			//int base = id;
			idmap.put(id, id);
			freq.put(id, 1);
			for(int j=0;j<step;j++){
				boolean ud = rn.nextFloat() < this.wrate;
				String query;
				if(ud){
					query = getUpdate(id);
				}else{
					query = getSelect(id);
				}
				pout.println(id + "\t" + String.valueOf((id % 5 + 1)) + "\t" + query);
				id++;
			}
			pout.println("COMMIT");
		}
		repQuery(freq,idmap);
	}
	
	private void genZipf() {
		Map<Integer, Integer> idmap = new HashMap<Integer, Integer>();
		Map<Integer, Integer> freq = new HashMap<Integer, Integer>();
		for(Integer i:Zipf){
			if(freq.containsKey(i)){
				freq.put(i, freq.get(i)+1);
			}else{
				freq.put(i, 1);
			}
		}
		int id=0;
		for(Integer i:freq.keySet()){
			idmap.put(i, id);
			id += step;
		}
		Random rn = new Random();
		rn.setSeed(System.currentTimeMillis());
		
		for(Integer i:Zipf){
			pout.println("BEGIN");
			int base = idmap.get(i);
			for(int j=0;j<step;j++){
				boolean ud = rn.nextFloat() < this.wrate;
				String query;
				if(ud){
					query = getUpdate(base);
				}else{
					query = getSelect(base);
				}
				pout.println(base + "\t" + String.valueOf((base % 5 + 1)) + "\t" + query);
				base++;
			}
			pout.println("COMMIT");
//			pout.println(base);
		}
		
		repQuery(freq, idmap);
		
	}
	
	private void repQuery(Map<Integer, Integer> freq, Map<Integer, Integer> idmap){
		
		FileOutputStream out_rep = null;
		PrintStream p_rep = null;
		
		List<Pair> txn = new ArrayList<Pair>();
		
		try {
        	String name = "Test1Workload.Repartition";
        	File f = new File(System.getProperty("user.home") + "/Documents/Result/" + name + "@" + String.valueOf(System.currentTimeMillis() / 10000));
        	
        	if(!f.exists())
        		f.createNewFile();
        	
			out_rep = new FileOutputStream((f), false);
			p_rep = new PrintStream(out_rep);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(Entry<Integer,Integer> e:freq.entrySet()){
			Pair p =new Pair(String.valueOf(idmap.get(e.getKey())),e.getValue());
			txn.add(p);
		}
		Collections.sort(txn);
		
		p_rep.println(step);
		
		for(Pair p:txn){
			p_rep.println(p.getKey() + "\t" + p.getValue());
		}
		
		p_rep.close();
		try {
			out_rep.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
	}
	
	private String getSelect(int key) {
		String sql = "SELECT * from load WHERE id = " + key;
		return sql;
	}

	private String getUpdate(int key) {
		String sql = "UPDATE load SET data = data WHERE id = " + key;
		return sql;
	}
	
}
