package utils;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Date;

public class ControlDB {
	private String source_db;
	private String target_db_name;
	private String table_name;
	private PreparedStatement ptmt=null;
	private ResultSet rs=null;
	private String sql="";
	 
	
	public ControlDB(String source_db, String target_db_name, String table_name) {
		this.source_db = source_db;
		this.target_db_name = target_db_name;
		this.table_name = table_name;
	}
	
		public String selectFileStatus(String field,String table, String data_file_name) {
			sql = "SELECT " + field + " FROM " +table + " WHERE file_name=?";
			System.out.println(sql);
			try {
				ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
				ptmt.setString(1,data_file_name);
				rs = ptmt.executeQuery();
				rs.next();
				return rs.getString(field);
			} catch (Exception e) {
				return null;
			} finally {
				try {
					if (ptmt != null)
						ptmt.close();
					if (rs != null)
						rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}

	}
	
		public int selectIDFile (String field, String table, String data_file_name){
			sql = "SELECT " + field + " FROM " +table + " WHERE file_name=?";
			System.out.println(sql);
			try {
				ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
				ptmt.setString(1,data_file_name);
				rs = ptmt.executeQuery();
				rs.next();
				return rs.getInt(field);
			} catch (Exception e) {
				return -1;
			} finally {
				try {
					if (ptmt != null)
						ptmt.close();
					if (rs != null)
						rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	public String selectField(String field, String config_name) {
		sql = "SELECT " + field + " FROM " + this.table_name + " WHERE config_name=?";
		try {
			ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
			ptmt.setString(1, config_name);
			rs = ptmt.executeQuery();
			rs.next();
			return rs.getString(field);
		} catch (Exception e) {
			return null;
		} finally {
			try {
				if (ptmt != null)
					ptmt.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

	}
	
	public boolean tableExists(String table_name) {
		try {
			DatabaseMetaData dbm = DBConnection.createConnection(this.target_db_name).getMetaData();
			ResultSet tables = dbm.getTables(null, null, table_name, null);
			try {
				if (tables.next()) {
					return true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

		return false;
	}
	public boolean insertValues(String column_list, String values, String target_table) {
		sql = "INSERT INTO " + target_table + "(" + column_list + ") VALUES " + values;
		System.out.println(sql);
		try {
			ptmt = DBConnection.createConnection(this.target_db_name).prepareStatement(sql);
			ptmt.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (ptmt != null)
					ptmt.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}
	public boolean insertLogFileStatus( String table, String file_name,String config_id, String file_status, String timestamp){
		sql = "INSERT INTO " + table
				+ "(file_name,data_file_config_id,file_status,staging_load_count,file_timestamp) value (?,?,?,?,?)";
		try {
			ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
			ptmt.setString(1, file_name);
			ptmt.setInt(2, Integer.parseInt(config_id));
			ptmt.setString(3, file_status);
			ptmt.setInt(4, Integer.parseInt("0"));
			ptmt.setString(5, timestamp);
			ptmt.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (ptmt != null)
					ptmt.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
		
	}

	public boolean updateLogAfterLoadingIntoStagingDB(String table, String file_status, String config_id, String timestamp,
			String stagin_load_count, String file_name) {
		  sql = "update "+table+ " set file_status=?, staging_load_count=?,  load_staging_timestamp=now() where file_name='" + file_name + "'";
		  System.out.println(sql);
		try {
			ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
			ptmt.setString(1, file_status);
			ptmt.setInt(2, Integer.parseInt(stagin_load_count));
//			ptmt.setString(4, timestamp);
			ptmt.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (ptmt != null)
					ptmt.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}

	public boolean createTable(String table_name, String variables, String column_list) {
		sql = "CREATE TABLE "+table_name+" (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,";
		String[] vari = variables.split(",");
		String[] col = column_list.split(",");
		for(int i =0;i<vari.length;i++) {
			sql+=col[i]+" "+vari[i]+ " NOT NULL,";
		}
		sql = sql.substring(0,sql.length()-1)+")";
		System.out.println(sql);
		try {
			ptmt = DBConnection.createConnection(this.target_db_name).prepareStatement(sql);
			ptmt.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (ptmt != null)
					ptmt.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}
	public String getSource_db() {
		return source_db;
	}
	public void setSource_db(String source_db) {
		this.source_db = source_db;
	}
	public String getTarget_db_name() {
		return target_db_name;
	}
	public void setTarget_db_name(String target_db_name) {
		this.target_db_name = target_db_name;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	
}
