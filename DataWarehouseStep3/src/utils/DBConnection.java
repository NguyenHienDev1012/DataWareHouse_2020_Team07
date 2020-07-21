package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	@SuppressWarnings("unused")
	public static Connection createConnection(String db_Name) {
		Connection con = null;
		String url = "jdbc:mysql://localhost:3306/" + db_Name;
		String user = "root";
		String password = "741852963";
		try {
			if (con == null || con.isClosed()) {
				Class.forName("com.mysql.jdbc.Driver");
				con = DriverManager.getConnection(url, user, password);
				System.out.println("connect success!");
				return con;

			} else {
				return con;
			}
		} catch (SQLException | ClassNotFoundException e) {
			System.out.println("can't connect");
			return null;
		}
	}
	@SuppressWarnings("unused")
	public static Connection createConnectionWithLogin(String db_Name,String user, String pass) {
		Connection con = null;
		String url = "jdbc:mysql://localhost:3306/" + db_Name;

		try {
			if (con == null || con.isClosed()) {
				Class.forName("com.mysql.jdbc.Driver");
				con = DriverManager.getConnection(url, user, pass);
				System.out.println("connect success!");
				return con;

			} else {
				return con;
			}
		} catch (SQLException | ClassNotFoundException e) {
			System.out.println("can't connect");
			return null;
		}
	}
	public static void main(String[] args) {
		DBConnection.createConnection("stagingdb");
	}
}
