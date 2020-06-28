package process3;
// create virtual data base staging

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


import utils.DBConnection;

public class VirtualDB {
	Connection conn;
//	làm thô thôi, chỉ lấy vài mẫu làm ví dụ nên thô, cứng
	public VirtualDB() {
		this.conn = DBConnection.createConnection("stagingdb");

	}

	public void copyDataStagingTXT() throws SQLException {
		Statement state = conn.createStatement();
		String sqlGet = "select * from student ";
		ResultSet rs = state.executeQuery(sqlGet);
		while (rs.next()) {
			
		String sql = "Insert into studentstaging ( stt, id_student, first_name, last_name,date_of_birth,class_id,class_name, phone, email,address,note) values (?,?,?,?,?,?,?,?,?,?,?) ";
        java.sql.PreparedStatement pre = conn.prepareStatement(sql);
        pre.setString(1, rs.getInt(1) +"");
        pre.setString(2, rs.getInt(2)+"");
        pre.setString(3, rs.getNString(3));
        pre.setString(4, rs.getNString(4));
        pre.setString(5, rs.getDate(5) +"");
        pre.setString(6, rs.getNString(6));
        pre.setString(7, rs.getNString(7));
        pre.setString(8, rs.getInt(8)+"");
        pre.setString(9, rs.getNString(9));
        pre.setString(10, rs.getNString(10));
        pre.setString(11, rs.getNString(11));
       pre.execute();
       
		}
		System.out.println("create staging done!");
		
	}
	public static void main(String[] args) throws SQLException {
		VirtualDB vdb = new VirtualDB();
		vdb.copyDataStagingTXT();
	}
}
