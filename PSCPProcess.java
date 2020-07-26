package download;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import control.SCP_DownLoad;
import utils.ControlDB;
import utils.DBConnection;

public class PSCPProcess {
	public DBConnection dbConnection=new DBConnection();
	public ControlDB controlDB=new ControlDB("controldb", "", "scp_download");
	public PreparedStatement ptmt=null;
	public ResultSet rs=null;
	public SCP_DownLoad scp_download;
	
	
// lấy tất cả config từ cơ sở dữ liệu download SCP từ cơ sở dữ liệu thay vì lấy từng field 
	public SCP_DownLoad selectAllField(int id_scp, String table) throws SQLException{
		String sql="SELECT * FROM "+table +" WHERE id= ?";
		System.out.println(sql);
		ptmt=DBConnection.createConnection(controlDB.getSource_db()).prepareStatement(sql);
		ptmt.setInt(1, id_scp);
		rs=ptmt.executeQuery();
		while(rs.next()){
			scp_download = new SCP_DownLoad(rs.getInt("id"), rs.getString("host_name"), rs.getInt("port_connect"),
					rs.getString("username"), rs.getString("pass"), rs.getString("file_name_architecture"),
					rs.getString("remotePath"), rs.getString("localPath"), rs.getInt("config_id"));
		}
		return scp_download;
	}
	
	public String selectField(int id_scp, String table, String field) throws SQLException{
		String sql = "SELECT " + field + " FROM " +table + " WHERE id=?";
		System.out.println(sql);
		try {
			ptmt=DBConnection.createConnection(controlDB.getSource_db()).prepareStatement(sql);
			ptmt.setInt(1, id_scp);
			rs=ptmt.executeQuery();
			rs.next();
			return rs.getString(field); 
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}finally {
			if(ptmt!=null){
				ptmt.close();
			}
			if(rs!=null){
				rs.close();
			}
		}
		
	}
	public int selectPortField(int id_scp, String table, String field) throws SQLException{
		String sql = "SELECT " + field + " FROM " +table + " WHERE id=?";
		System.out.println(sql);
		try {
			ptmt=DBConnection.createConnection(controlDB.getSource_db()).prepareStatement(sql);
			ptmt.setInt(1, id_scp);
			rs=ptmt.executeQuery();
			rs.next();
			return rs.getInt(field); 
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}finally {
			if(ptmt!=null){
				ptmt.close();
			}
			if(rs!=null){
				rs.close();
			}
		}
		
	}
	
	

}
