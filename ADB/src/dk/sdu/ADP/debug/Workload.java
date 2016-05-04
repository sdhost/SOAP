package dk.sdu.ADP.debug;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.JTA.JTA;

public class Workload {
	
	private ADP adp = null;
	private FileOutputStream out;
	private PrintStream pout;
	private int tranc, queryc;
	private double wrate;
	private int par;
	private HashMap<String, Vector<String>> parMenu;
	private HashMap<String, Integer> parSize;
	private int single;
	
	public Workload(int par, int tcount, int qcount, int type, double write){
		this.par = par;
		  
		Vector<Properties> inis = JTA.genBTM(par);
		adp = new ADP(inis,"FileUndefined",1000000,1000);
		
		adp.lookupTable.tuplePK = new HashMap<String, ArrayList<String>>();
		adp.lookupTable.tupleTable = new HashMap<String, String>();
		
		System.out.println("Start Load lookupTable");
        File lookup_addr = new File(System.getProperty("user.home") + "/Documents/Result/","lookuptable.ec2.jta.dump");
        
        try {
			adp.lookupTable.loadLookupTable(lookup_addr.toString());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
        System.out.println("End Load lookupTable");
        
        parMenu = new HashMap<String, Vector<String>>();
        parSize = new HashMap<String, Integer>();
        parMenu = adp.lookupTable.getParMenu();
        
        for(String s:parMenu.keySet()){
        	parSize.put(s, parMenu.get(s).size());
        }

        
        try {
        	String name = String.valueOf(tcount/1000) + "." + String.valueOf(qcount/1000) + "t" + String.valueOf(type);
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
        
        this.tranc = tcount;
        this.queryc = qcount;
        this.wrate = write;
        
        System.out.println("Finished Initialization!");
        gen(type);
        pout.close();
        try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        System.out.println("Finished Generation!");
	}

	private void gen(int type) {
		/*	Type 0: Single Partitioned transaction for all
		 * 	Type 1: Random workload
		 * 	Type 2: Skewed workload
		 */
		Random rn = new Random();
		rn.setSeed(System.currentTimeMillis());
		
		
		if(type == 0){
			single = this.tranc;
			for(int i=0;i<this.tranc;i++){
				pout.println("BEGIN");
				String sele = String.valueOf(rn.nextInt(par) + 1);
				for(int j=0;j<this.queryc;j++){
					boolean ud = rn.nextFloat() < this.wrate;
					int max = parSize.get(sele);
					int pos = rn.nextInt(max);
					String key = parMenu.get(sele).elementAt(pos);
					String query;
					if(ud){
						query = getUpdate(key);
					}else{
						query = getSelect(key);
					}
					pout.println(key + "\t" + sele + "\t" + query);
				}
				pout.println("COMMIT");
			}
		}else if(type == 1){
			
			for(int i=0;i<this.tranc;i++){
				pout.println("BEGIN");
				String last_par = null;
				boolean isSingle = true;
				for(int j=0;j<this.queryc;j++){
					String sele = String.valueOf(rn.nextInt(par) + 1);
					if(last_par == null)
						last_par = sele;
					else{
						if(!last_par.equals(sele)){
							isSingle = false;
						}
					}
					boolean ud = rn.nextFloat() < this.wrate;
					int max = parSize.get(sele);
					int pos = rn.nextInt(max);
					String key = parMenu.get(sele).elementAt(pos);
					String query;
					if(ud){
						query = getUpdate(key);
					}else{
						query = getSelect(key);
					}
					pout.println(key + "\t" + sele + "\t" + query);
				}
				if(isSingle)
					single++;
				pout.println("COMMIT");
			}
		}else if(type == 2){
			
		}
		
	}
	
	public int getSingle(){
		return this.single;
	}
	
	

	private String getSelect(String key) {
		String sql = "SELECT * from ";
		String table = adp.lookupTable.tupleTable.get(key);
		sql += table + " WHERE ";
		for(int i=0;i< adp.lookupTable.keylist.get(table).size();i++){
			if(i>0)
				sql += "AND ";
			sql += adp.lookupTable.keylist.get(table).get(i) + "=" + adp.lookupTable.tuplePK.get(key).get(i) + " ";
		}
		return sql;
	}

	private String getUpdate(String key) {
		String sql = "UPDATE";
		String table = adp.lookupTable.tupleTable.get(key);
		sql += " " + table + " SET ";
		if(table.equalsIgnoreCase("warehouse")){
			sql += "w_ytd = w_ytd WHERE ";
		}else if(table.equalsIgnoreCase("district")){
			sql += "d_ytd = d_ytd WHERE ";
		}else if(table.equalsIgnoreCase("customer")){
			sql += "c_balance = c_balance WHERE ";
		}else if(table.equalsIgnoreCase("history")){
			sql += "h_amount = h_amount WHERE ";
		}else if(table.equalsIgnoreCase("oorder")){
			sql += "o_ol_cnt = o_ol_cnt WHERE ";
		}else if(table.equalsIgnoreCase("new_order")){
			sql += "no_o_id = no_o_id WHERE ";
		}else if(table.equalsIgnoreCase("order_line")){
			sql += "ol_amount = ol_amount WHERE ";
		}else if(table.equalsIgnoreCase("stock")){
			sql += "s_ytd = s_ytd WHERE ";
		}else if(table.equalsIgnoreCase("item")){
			sql += "i_price = i_price WHERE ";
		}else{
			System.err.println("Error, unknown table " + table);
			return null;
		}
		
		for(int i=0;i< adp.lookupTable.keylist.get(table).size();i++){
			if(i>0)
				sql += "AND ";
			sql += adp.lookupTable.keylist.get(table).get(i) + "=" + adp.lookupTable.tuplePK.get(key).get(i) + " ";
		}
		
		return sql;
	}
}
