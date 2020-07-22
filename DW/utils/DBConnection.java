package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DBConnection {
	@SuppressWarnings("unused")
	public static Connection createConnection(String db_Name) {
		Connection con = null;
		String url = "jdbc:mysql://localhost:3306/" + db_Name;
		String user = "root";
		String password = "1012";
		try {
			if (con == null || con.isClosed()) {
				Class.forName("com.mysql.jdbc.Driver");
				con = DriverManager.getConnection(url, user, password);
				return con;

			} else {
				return con;
			}
		} catch (SQLException | ClassNotFoundException e) {
			return null;
		}
	}
	public static Connection createConnectionWithURLLogin(String url, String db_Name, String user,
			String password) {
		Connection connect= null;
		String urls =  url+ db_Name;
		String users = user;
		String passwords = password;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection(urls, users, passwords);
			return connect;
		} catch (SQLException | ClassNotFoundException e) {
			return null;
		}
	}
	public static void main(String[] args) {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
	    String timestamp = dtf.format(now);
		System.out.println(timestamp);
		
	}
}
