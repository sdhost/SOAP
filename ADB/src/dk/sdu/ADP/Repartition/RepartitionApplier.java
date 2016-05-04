package dk.sdu.ADP.Repartition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import pojo.*;

import dk.sdu.ADP.ADP;
import dk.sdu.ADP.Model.TuplePosition;
import dk.sdu.ADP.util.Pair;
import dk.sdu.ADP.util.PropertySet;

public class RepartitionApplier implements Runnable {
	
	RepartitionSearcher rs;
	ADP adp;
	double current_cost;
	double threshold;
	ResultSet result = null;
	PreparedStatement getWarehouse = null;
	PreparedStatement setWarehouse = null;
	PreparedStatement getDistrict = null;
	PreparedStatement setDistrict = null;
	PreparedStatement getCustomer = null;
	PreparedStatement setCustomer = null;
	PreparedStatement getHistory = null;
	PreparedStatement setHistory = null;
	PreparedStatement getOorder = null;
	PreparedStatement setOorder = null;
	PreparedStatement getNorder = null;
	PreparedStatement setNorder = null;
	PreparedStatement getOrderline = null;
	PreparedStatement setOrderline = null;
	PreparedStatement getStock = null;
	PreparedStatement setStock = null;
	PreparedStatement getitem = null;
	PreparedStatement setitem = null;
	
	
	
	
	
	public RepartitionApplier(ADP adp, int timeInterval, double threshold){
		//For repartitioning applier thread
		//Currently not used, without initialize the prepared statement
		rs = new RepartitionSearcher(adp,timeInterval);
		this.adp = adp;
		this.current_cost = adp.model.getDesignCost();
		this.threshold = threshold;
	}
	
	public RepartitionApplier(ADP adp){
		//For debug usage
		rs = null;
		this.adp = adp;
		this.current_cost = adp.model.getDesignCost();
		this.threshold = -1;
	}
	
	@Override
	public void run() {
		Thread t = new Thread(rs);
		t.start();
		while(true){
			//synchronized(rs.change){
//				while(rs.hasNew == false){
//					try {
//						rs.change.wait();
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
			//}
			//System.out.println("Applier Activated!");
			if(rs.hasNew == false){
				System.out.println("Will not happen");
				continue;
			}
			rs.hasNew = false;
			if(this.current_cost - rs.last_cost < threshold)
				continue;
			Set<Pair> changelist = rs.change;
			try {
				Apply(changelist, rs.last_design, rs.last_load, rs.last_cost);
				rs.change.clear();
				this.current_cost = rs.last_cost;
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			
		}
		
	}

	public void Apply(Set<Pair> changelist, HashMap<String, Map<String, PropertySet<String>>> last_design, LinkedList<HashMap<String, List<String>>> last_load, double last_cost) throws SQLException {
		int i = 0;
		for(Pair p:changelist){
			i++;
			String key = p.getKey();
			String elem[] = key.split("\t");
			String gain = elem[1];
			key = elem[0];
			boolean delete = false;
			if(p.getValue() < 0)
				delete = true;
			String par = String.valueOf(Math.abs(p.getValue()));
			
			String sql = null;
			if(delete){
				//Skip delete for easy of experiment
				
//				if(adp.lookupTable.get(key).contains(par)){
//					 sql = CreateDelete(key, par);
//					 adp.lookupTable.delete(key, par, -p.getValue());// Keep all the replicas, but only update the ones we think it is not deleted
//					 adp.stmt.addBatch(sql);
//				}
//				 if(i % 10000 == 0){
//						adp.stmt.executeBatch();
//						adp.conn.commit();
//				}
			}else{
				if(adp.lookupTable.get(key).contains(par))
					continue;
				CreateInsert(key, par);//Will execute the insert operation in this function
				adp.lookupTable.insert(key, par);
			}
			
				
			
			
			
		}
		
		adp.stmt.executeBatch();
		adp.conn.commit();
		adp.model.updateDesign(last_design, last_load);

		
	}
	
	private String execInsert(TuplePosition primaryKey, String par) throws SQLException{
		//Map<String,String> fulcol = new HashMap<String, String>();
		String table = primaryKey.table.toLowerCase();
		if(table.equals("warehouse")){
			if(this.setWarehouse == null){
				this.setWarehouse = adp.conn.prepareStatement(
						"INSERT INTO warehouse " +
						        " (w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, partition) " +
						        "SELECT w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, ? " +
						        "FROM warehouse " +
						        "WHERE w_id = ?");
			}
			
			setWarehouse.setLong(1, Integer.valueOf(par));
            setWarehouse.setLong(2, Integer.valueOf(primaryKey.cols.get("w_id")));
            int rtn = 0;
            rtn = setWarehouse.executeUpdate();
            if(rtn == 0){
            	System.out.println("Cannot update w_id=" + primaryKey.cols.get("w_id") + ", partition=" + par + " in table warehouse");
            	return null;
            }
            	
			return setWarehouse.toString();	
			
		}else if(table.equals("district")){
			
			if(this.setDistrict == null){
				this.setDistrict = adp.conn.prepareStatement(
						"INSERT INTO district " +
								" (d_id, d_w_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, partition) " +
								"SELECT d_id, d_w_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, ? " + 
								"FROM district " + 
								"WHERE d_w_id = ? AND d_id = ?");
			}
			
			this.setDistrict.setLong(1, Integer.valueOf(par));
			this.setDistrict.setLong(2, Integer.valueOf(primaryKey.cols.get("d_w_id")));
			this.setDistrict.setLong(3, Integer.valueOf(primaryKey.cols.get("d_id")));
			
            int rtn = 0;
            rtn = setDistrict.executeUpdate();
            if(rtn == 0){
            	System.out.println("Cannot update d_w_id=" + primaryKey.cols.get("d_w_id") + ", d_id=" + primaryKey.cols.get("d_id") + ", partition=" + par +" in table district");
				return null;
            }
            
            return this.setDistrict.toString();
			
		}else if(table.equals("customer")){
			if(this.setCustomer == null){
				this.setCustomer = adp.conn.prepareStatement(
						"INSERT INTO customer " +
								" (c_id, c_d_id, c_w_id, " +
						         "c_discount, c_credit, c_last, c_first, c_credit_lim, " +
						         "c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, " +
						         "c_street_1, c_street_2, c_city, c_state, c_zip, " +
						         "c_phone, c_since, c_middle, c_data, partition) " +
						         "SELECT c_id, c_d_id, c_w_id," +
						         "c_discount, c_credit, c_last, c_first, c_credit_lim, " +
						         "c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, " +
						         "c_street_1, c_street_2, c_city, c_state, c_zip, " +
						         "c_phone, c_since, c_middle, c_data, ? " +
						         "FROM customer " + 
						         "WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?");
			}
			this.setCustomer.setLong(1, Integer.valueOf(par));
			this.setCustomer.setLong(2, Integer.valueOf(primaryKey.cols.get("c_id")));
			this.setCustomer.setLong(3, Integer.valueOf(primaryKey.cols.get("c_d_id")));
			this.setCustomer.setLong(4, Integer.valueOf(primaryKey.cols.get("c_w_id")));
			
			int rtn = 0;
            rtn = setCustomer.executeUpdate();
            if(rtn == 0){
            	System.out.println("Cannot update c_id=" + primaryKey.cols.get("c_id") + 
            			", c_d_id=" + primaryKey.cols.get("c_d_id") + 
            			", c_w_id=" + primaryKey.cols.get("c_w_id") +
            			", partition=" + par +" in table customer");
				return null;
            }
            
            return this.setCustomer.toString();
			
		}else if(table.equals("history")){
			if(this.setHistory == null){
				this.setHistory = adp.conn.prepareStatement(
						"INSERT INTO history " +
							       " (h_c_id, h_c_d_id, h_c_w_id, " +
							         "h_d_id, h_w_id, " +
							         "h_date, h_amount, h_data, partition) " +
							         "SELECT h_c_id, h_c_d_id, h_c_w_id, " +
							         "h_d_id, h_w_id, " +
							         "h_date, h_amount, h_data, ? " + 
							         "FROM history " +
							         "WHERE h_c_id = ? AND h_c_d_id = ? AND h_c_w_id = ? AND h_d_id = ? AND h_w_id = ?");
			}
			this.setHistory.setLong(1, Integer.valueOf(par));
			this.setHistory.setLong(2, Integer.valueOf(primaryKey.cols.get("h_c_id")));
			this.setHistory.setLong(3, Integer.valueOf(primaryKey.cols.get("h_c_d_id")));
			this.setHistory.setLong(4, Integer.valueOf(primaryKey.cols.get("h_c_w_id")));
			this.setHistory.setLong(5, Integer.valueOf(primaryKey.cols.get("h_d_id")));
			this.setHistory.setLong(6, Integer.valueOf(primaryKey.cols.get("h_w_id")));
			
			int rtn = 0;
            rtn = setHistory.executeUpdate();
            if(rtn == 0){
            	System.out.println("Cannot update h_c_id=" + primaryKey.cols.get("h_c_id") + 
            			", h_c_d_id=" + primaryKey.cols.get("h_c_d_id") + 
            			", h_c_w_id=" + primaryKey.cols.get("h_c_w_id") +
            			", h_d_id=" + primaryKey.cols.get("h_d_id") +
            			", h_w_id=" + primaryKey.cols.get("h_w_id") +
            			", partition=" + par +" in table history");
				return null;
            }
            
            return this.setHistory.toString();
			
		}else if(table.equals("oorder")){
			if(this.setOorder == null){
				this.setOorder = adp.conn.prepareStatement(
						"INSERT INTO oorder " +
							       " (o_id, o_w_id,  o_d_id, o_c_id, " +
							         "o_carrier_id, o_ol_cnt, o_all_local, o_entry_d, partition) " +
							       "SELECT o_id, o_w_id,  o_d_id, o_c_id, " +
							         "o_carrier_id, o_ol_cnt, o_all_local, o_entry_d, ? " +
							       "FROM oorder " + 
							        "WHERE o_id=? AND o_w_id=? AND o_c_id=?");
			}
			this.setOorder.setLong(1, Integer.valueOf(par));
			this.setOorder.setLong(2, Integer.valueOf(primaryKey.cols.get("o_id")));
			this.setOorder.setLong(3, Integer.valueOf(primaryKey.cols.get("o_w_id")));
			this.setOorder.setLong(4, Integer.valueOf(primaryKey.cols.get("o_d_id")));
			
			int rtn = 0;
            rtn = setOorder.executeUpdate();
            if(rtn == 0){
            	System.out.println("Cannot update o_id=" + primaryKey.cols.get("o_id") + 
            			", o_w_id=" + primaryKey.cols.get("o_w_id") + 
            			", o_d_id=" + primaryKey.cols.get("o_d_id") +
            			", partition=" + par +" in table Oorder");
				return null;
            }
            
            return this.setOorder.toString();
			
		}else if(table.equals("new_order")){
			if(this.setNorder == null){
				this.setNorder = adp.conn.prepareStatement(
						"INSERT INTO new_order " +
							       " (no_w_id, no_d_id, no_o_id, partition) " +
								   "SELECT no_w_id, no_d_id, no_o_id, ? " + 
							       "FROM new_order " +
								   "WHERE no_w_id=? AND no_d_id=? AND no_o_id=?");
			}
			this.setNorder.setLong(1, Integer.valueOf(par));
			this.setNorder.setLong(2, Integer.valueOf(primaryKey.cols.get("no_w_id")));
			this.setNorder.setLong(3, Integer.valueOf(primaryKey.cols.get("no_d_id")));
			this.setNorder.setLong(4, Integer.valueOf(primaryKey.cols.get("no_o_id")));
			
			int rtn = 0;
            rtn = setNorder.executeUpdate();
            if(rtn == 0){
            	System.out.println("Cannot update no_o_id=" + primaryKey.cols.get("no_o_id") + 
            			", no_w_id=" + primaryKey.cols.get("no_w_id") + 
            			", no_d_id=" + primaryKey.cols.get("no_d_id") +
            			", partition=" + par +" in table new_order");
				return null;
            }
            
            return this.setNorder.toString();
			
		}else if(table.equals("order_line")){
			if(this.setOrderline == null){
				this.setOrderline = adp.conn.prepareStatement(
						"INSERT INTO order_line " +
							       " (ol_w_id, ol_d_id, ol_o_id, " +
							         "ol_number, ol_i_id, ol_delivery_d, " +
							         "ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, partition) " +
							         "SELECT ol_w_id, ol_d_id, ol_o_id, " +
							         "ol_number, ol_i_id, ol_delivery_d, " +
							         "ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info,? " +
							         "FROM order_line " + 
							         "WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=? AND ol_number=?");
			}
			this.setOrderline.setLong(1, Integer.valueOf(par));
			this.setOrderline.setLong(2, Integer.valueOf(primaryKey.cols.get("ol_w_id")));
			this.setOrderline.setLong(3, Integer.valueOf(primaryKey.cols.get("ol_d_id")));
			this.setOrderline.setLong(4, Integer.valueOf(primaryKey.cols.get("ol_o_id")));
			this.setOrderline.setLong(5, Integer.valueOf(primaryKey.cols.get("ol_number")));
			
			int rtn = 0;
            rtn = setOrderline.executeUpdate();
            if(rtn == 0){
            	System.out.println("Cannot update ol_o_id=" + primaryKey.cols.get("ol_o_id") + 
            			", ol_w_id=" + primaryKey.cols.get("ol_w_id") + 
            			", ol_d_id=" + primaryKey.cols.get("ol_d_id") +
            			", ol_number=" + primaryKey.cols.get("ol_number") +
            			", partition=" + par +" in table order_line");
				return null;
            }
            
            return this.setOrderline.toString();
			
			
		}else if(table.equals("stock")){
			if(this.setStock == null){
				this.setStock = adp.conn.prepareStatement(
						"INSERT INTO stock " +
							       " (s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, " +
							         "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
							         "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, partition) " +
							         "SELECT s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, " +
							         "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
							         "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ? " +
							         "FROM stock " +
							         "WHERE s_i_id=? AND s_w_id=?");
			}
			
			this.setStock.setLong(1, Integer.valueOf(par));
			this.setStock.setLong(2, Integer.valueOf(primaryKey.cols.get("s_i_id")));
			this.setStock.setLong(3, Integer.valueOf(primaryKey.cols.get("s_w_id")));
			
			int rtn = 0;
            rtn = setStock.executeUpdate();
            if(rtn == 0){
            	System.out.println("Cannot update si_i_id=" + primaryKey.cols.get("si_i_id") + 
            			", s_w_id=" + primaryKey.cols.get("s_w_id") + 
            			", partition=" + par +" in table stock");
				return null;
            }
            
            return this.setStock.toString();
			
		}else if(table.equals("item")){
			if(this.setitem == null){
				this.setitem = adp.conn.prepareStatement(
						"INSERT INTO item " +
							       " (i_id, i_name, i_price, i_data, i_im_id, partition) " +
								"SELECT i_id, i_name, i_price, i_data, i_im_id, ? " + 
							       "FROM item " +
								"WHERE i_id=? ");
						
			}
			
			this.setitem.setLong(1, Integer.valueOf(par));
			this.setitem.setLong(2, Integer.valueOf(primaryKey.cols.get("i_id")));
			
			int rtn = 0;
            rtn = setitem.executeUpdate();
            if(rtn == 0){
            	System.out.println("Cannot update i_id=" + primaryKey.cols.get("i_id") + 
            			", partition=" + par +" in table item");
				return null;
            }
            
            return this.setitem.toString();
			
		}else{
			System.out.println("Cannot find table " + table + " in system");
			return null;
		}
	}
	
	private String CreateInsert(String key, String par) throws SQLException {
		if(adp.lookupTable.get(key).contains(par))
			return null;
		TuplePosition pos = adp.lookupTable.getRaw(key);
		String sql = null;
		try{
			sql = this.execInsert(pos, par);
		}catch(SQLException e){
			e.printStackTrace();
			if(e.getNextException() != null){
				e.printStackTrace();
			}
			adp.conn.rollback();
		}
		return sql;
	}

	private String CreateDelete(String key, String par) {
		return adp.lookupTable.getSQL(key, par, "DELETE");
	}

	public Vector<String> getQuery(Set<Pair> change) {
		Vector<String> vq = new Vector<String>();
		for(Pair p:change){
			String key = p.getKey();
			String elem[] = key.split("\t");
			String gain = elem[1];
			key = elem[0];
			boolean delete = false;
			if(p.getValue() < 0)
				delete = true;
			String par = String.valueOf(Math.abs(p.getValue()));
			String opar = adp.lookupTable.get(key).iterator().next();
			
			String sql = null;
			if(delete){
				
				if(adp.lookupTable.get(key).contains(par)){
					 sql = getDeleteJTA(key, par);
					 vq.add(par + "\t" + key +"\t" + gain + "\t" + sql);
				}
			}else{
				if(adp.lookupTable.get(key).contains(par))
					continue;
				sql = getSelectJTA(key);
				try {
					Connection c = adp.jta.getConnection(Integer.valueOf(opar));
					Statement st = c.createStatement();
					ResultSet res = st.executeQuery(sql);
					if(!res.next()){
						System.out.println("Tuple not found in Partition: " + opar);
						System.out.println(sql);
						st.close();
						c.close();
						continue;
					}
					String table = adp.lookupTable.getTableName(key);
					if(table.equalsIgnoreCase("customer")){
						Customer data = new Customer();
						data.assignVal(res);
						sql = data.getInsert();		
					}else if(table.equalsIgnoreCase("district")){
						District data = new District();
						data.assignVal(res);
						sql = data.getInsert();		
					}else if(table.equalsIgnoreCase("history")){
						History data = new History();
						data.assignVal(res);
						sql = data.getInsert();		
					}else if(table.equalsIgnoreCase("item")){
						Item data = new Item();
						data.assignVal(res);
						sql = data.getInsert();		
					}else if(table.equalsIgnoreCase("New_Order")){
						NewOrder data = new NewOrder();
						data.assignVal(res);
						sql = data.getInsert();		
					}else if(table.equalsIgnoreCase("Oorder")){
						Oorder data = new Oorder();
						data.assignVal(res);
						sql = data.getInsert();		
					}else if(table.equalsIgnoreCase("Order_line")){
						OrderLine data = new OrderLine();
						data.assignVal(res);
						sql = data.getInsert();		
					}else if(table.equalsIgnoreCase("Stock")){
						Stock data = new Stock();
						data.assignVal(res);
						sql = data.getInsert();		
					}else if(table.equalsIgnoreCase("Warehouse")){
						Warehouse data = new Warehouse();
						data.assignVal(res);
						sql = data.getInsert();		
					}else{
						System.out.println("Unknow table name: " + table);
					}
					
					res.close();
					st.close();
					c.close();
					
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				//adp.lookupTable.insert(key, par);
				vq.add(par + "\t" + key + "\t" + gain + "\t" + sql);
			}
		}
		
		return vq;
		
	}
	private String getInsertJTA(String key) {
		return adp.lookupTable.getSQLJTA(key, "INSERT");
	}

	private String getDeleteJTA(String key, String par) {
		return adp.lookupTable.getSQLJTA(key, "DELETE");
	}
	
	private String getSelectJTA(String key){
		return adp.lookupTable.getSQLJTA(key, "SELECT");
	}
	

}
