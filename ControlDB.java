package utils;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import com.mysql.jdbc.Connection;

import control.Configuration;
import control.Log_file;
import date_dim.Date_Dim;
import etl.DataProcess;
import mail.MailConfig;
import mail.SendMail;

public class ControlDB {
	private String source_db;
	private String target_db_name;
	private String table_name;
	private PreparedStatement ptmt = null;
	private ResultSet rs = null;
	private String sql = "";
	private Configuration configuration;
	
	public ControlDB(String source_db, String target_db_name, String table_name) {
		this.source_db = source_db;
		this.target_db_name = target_db_name;
		this.table_name = table_name;
	}

	public ControlDB() {

	}

	public String selectFileStatus(String field, String table, String data_file_name) {
		sql = "SELECT " + field + " FROM " + table + " WHERE file_name=?";
		System.out.println(sql);
		try {
			ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
			ptmt.setString(1, data_file_name);
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

	public int selectIDFile(String field, String table, String data_file_name) {
		sql = "SELECT " + field + " FROM " + table + " WHERE file_name=?";
		System.out.println(sql);
		try {
			ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
			ptmt.setString(1, data_file_name);
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
	// Phương thức lấy thời gian hiện tại.
		public String getCurrentTime() {
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
			LocalDateTime now = LocalDateTime.now();
			return dtf.format(now);
		}
			
	// Lấy tất cả các trường từ cơ sở dữ liệu từ bảng configuration dựa vào
	// config_name
	public Configuration selectAllFieldConfiguration(String config_name) throws SQLException {
		String sql = "SELECT * FROM " + this.table_name + " WHERE config_name=?";
		ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
		ptmt.setString(1, config_name);
		rs = ptmt.executeQuery();
		while (rs.next()) {
			configuration = new Configuration(rs.getInt("config_id"), rs.getString("config_name"),
					rs.getString("config_des"), rs.getString("target_table"), rs.getString("file_type"),
					rs.getString("import_dir"), rs.getString("success_dir"), rs.getString("error_dir"),
					rs.getString("column_list"), rs.getString("delimmiter"));
		}
		return configuration;

	}

	//// Lấy tất cả các trường từ cơ sở dữ liệu từ bảng configuration dựa vào
	//// config_name
	public Configuration selectAllFieldConfigurationByConfigId(int config_id) throws SQLException {
		String sql = "SELECT * FROM " + this.table_name + " WHERE config_id=?";
		ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
		ptmt.setInt(1, config_id);
		rs = ptmt.executeQuery();
		while (rs.next()) {
			configuration = new Configuration(rs.getInt("config_id"), rs.getString("config_name"),
					rs.getString("config_des"), rs.getString("target_table"), rs.getString("file_type"),
					rs.getString("import_dir"), rs.getString("success_dir"), rs.getString("error_dir"),
					rs.getString("column_list"), rs.getString("delimmiter"));
		}
		return configuration;

	}

	// Lấy tất cả các trường từ cơ sở dữ liệu từ bảng log_file dựa vào file_name
	public Log_file selectAllFieldLogFile(String file_name, String table_name) throws SQLException {
		String sql = "SELECT * FROM " + table_name + " WHERE file_name=?";
		System.out.println(sql);
		ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
		ptmt.setString(1, file_name);
		rs = ptmt.executeQuery();
		while (rs.next()) {
			Log_file log_file = new Log_file(rs.getInt("data_file_id"), rs.getString("file_name"),
					rs.getInt("data_file_config_id"), rs.getString("file_status"), rs.getInt("staging_load_count"),
					rs.getDate("file_timestamp"), rs.getDate("load_staging_timestamp"), null);

			return log_file;
		}
		return null;

	}

	// Lấy tất cả các file_name trong cơ sở dữ liệu từ bảng log_file hien co
	public ArrayList<String> selectAllFileNameInLogFile(String table_name) throws SQLException {
		String sql = "SELECT * FROM " + table_name;
		System.out.println(sql);
		ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
		rs = ptmt.executeQuery();
		ArrayList<String> listFile_name = new ArrayList<>();
		while (rs.next()) {

			listFile_name.add(rs.getString("file_name"));
		}
		return listFile_name;

	}

	// Lấy tất cả config_name từ bảng configuration
	public ArrayList<String> getAllConfigName() throws SQLException {
		String sql = "SELECT config_name FROM " + this.table_name;
		ArrayList<String> listConfigname = new ArrayList<>();
		ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
		rs = ptmt.executeQuery();
		while (rs.next()) {
			listConfigname.add(rs.getString("config_name"));
		}
		return listConfigname;

	}

	public static void main(String[] args) throws SQLException {
		ControlDB controlDb = new ControlDB("controldb", "stagingdb", "configuration");
//		Configuration c = controlDb.selectAllFieldConfiguration("file_student_xlsx");
		// System.out.println(controlDb.selectAllFieldLogFile("monhoc2013.xlsx",
		// "log_file"));
//		System.out.println(controlDb.selectAllFileNameInLogFile("log_file"));
		// System.out.println(controlDb.getAllConfigName().size());
		controlDb.updateLogAfterLoadingIntoDW(2, "TShjj");
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


	public boolean loadDateDimToDW(String target_table) {
		System.out.println(sql);
		String sqlLoad = "INSERT INTO " + target_table + " VALUES" + Date_Dim.loadDateDimDW();
		System.out.println("sqlLoad: " + sqlLoad);
		try {
			ptmt = DBConnection.createConnection("datawarehouse").prepareStatement(sqlLoad);
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

	public boolean insertLogFileStatus(String table, String file_name, int config_id, String file_status,
			String timestamp) {
		sql = "INSERT INTO " + table
				+ "(file_name,data_file_config_id,file_status,staging_load_count,file_timestamp) value (?,?,?,?,?)";
		try {
			ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
			ptmt.setString(1, file_name);
			ptmt.setInt(2, config_id);
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

	public boolean updateLogAfterLoadingIntoStagingDB(String table, String file_status, String timestamp,
			String stagin_load_count, String file_name) {
		sql = "update " + table
				+ " set file_status=?, staging_load_count=?,  load_staging_timestamp=now() where file_name='"
				+ file_name + "'";
		System.out.println(sql);
		try {
			ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
			ptmt.setString(1, file_status);
			ptmt.setInt(2, Integer.parseInt(stagin_load_count));
			// ptmt.setString(4, timestamp);
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

	public boolean updateLogAfterLoadingIntoDW(int fileId, String file_status) {
		sql = "update " + " log_file " + " set file_status = ?,  load_warehouse_timestamp = now() where data_file_id = "
				+ fileId;
		System.out.println(sql);
		try {
			ptmt = DBConnection.createConnection(this.source_db).prepareStatement(sql);
			ptmt.setString(1, file_status);
			// ptmt.setString(4, timestamp);
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

	public void truncateTable(String db_name, String table_name) {
		String sql;
		Connection connection = null;
		PreparedStatement pst = null;
		try {
			sql = "TRUNCATE " + table_name;
			connection = (Connection) DBConnection.createConnection(db_name);
			pst = connection.prepareStatement(sql);
			pst.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (connection != null)
					connection.close();
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
