

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;


public class Load implements Serializable {

  public int   id; // PRIMARY KEY
  public int   data;


  public String toString()
  {
    return (
      "\n***************** Item ********************" +
      "\n*    id = " + id +
      "\n*  data = " + data +
      "\n**********************************************"
      );
  }
  public boolean assignVal(ResultSet rs) throws SQLException{
	  if(rs == null)
		  return true;
	  this.id = rs.getInt("id");
	  this.data = rs.getInt("data");
	  return false;
  
  }
  
  public String getInsert(){
	  String sql = "INSERT INTO load " +
		       " (id, data) " +
		       "VALUES (";
	  sql += this.id + ",";
	  sql += this.data + ")";
	  return sql;
  }
  
  public String getSelect(){
	  String sql = "SELECT * from load " +
			  		"WHERE id = " + id;
	  return sql;
  }

}  // end Load