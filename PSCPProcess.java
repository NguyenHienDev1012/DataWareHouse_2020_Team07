package process;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import notification.SendMail;
import utils.ControlDB;
import utils.DBConnection;
public class PSCPProcess {
	public DBConnection dbConnection=new DBConnection();
	public ControlDB controlDB=new ControlDB("controldb", "", "scp_download");
	public PreparedStatement ptmt=null;
	public ResultSet rs=null;
	private String emailAddress="nguyenthanhhien.itnlu@gmail.com";
	
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
