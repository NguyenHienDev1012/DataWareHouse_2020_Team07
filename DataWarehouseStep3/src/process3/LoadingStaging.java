package process3;
// step 3 int ETL , load data from starging into data warehouse

// định comment tiếng anh mà làm biếng nên thôi, dẹp đê!

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;

import utils.DBConnection;

public class LoadingStaging {
	Connection connStaging;
	Connection connDW;
	List<String> listField;
	List<String> listFieldFormat;
	String tabelStaging;
	String tableDW;
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	java.util.Date utilDate = new java.util.Date();
//@SuppressWarnings("deprecation")
//Date utilDate2 = new Date(9999, 10, 10);

//	LocalDate dateMax = LocalDate.MAX; // xài max mysql không chịu nổi.
	String fields;
	String v;
	String fields2;

	// Bước 1: kết nôi với dataWarehouse
	public LoadingStaging(int indextConfigFormat) throws SQLException {
		this.connDW = DBConnection.createConnection("DataWarehouse");
		init(indextConfigFormat);

	}
	//	Bước 2 : lấy thông tin data staging từ table config
	private void init(int indextConfigFormat) throws SQLException {
		Statement stateD = connDW.createStatement();
		String sql = "SELECT * FROM configformat WHERE configformat.stt = " + indextConfigFormat;
		ResultSet rsConfig = stateD.executeQuery(sql);
		rsConfig.next();
		this.connStaging = DBConnection.createConnection(rsConfig.getString(2));
		// lấy các giá trị chuẩn bị cho câu query của dataWarehouse
		tabelStaging = rsConfig.getString(5);
		tableDW = rsConfig.getString(8);
		listField = new LinkedList<String>();
		String[] lf = rsConfig.getString(6).split(",");
		for (String s : lf) {
			listField.add(s);
		}
		listFieldFormat = new LinkedList<String>();
		String[] lff = rsConfig.getString(7).split(",");
		for (String s : lff) {
			listFieldFormat.add(s);
		}
		// chuẩn bị câu query data warehouse
		fields = "(";
		v = "(";
		for (String s : listField) {
			fields += s + ",";
			v += "?,";
		}
		fields2 = fields.substring(1, fields.length() - 1);
		fields += "date_expire)";
		v += "?)";
	}
	// Bước 3: kết nối với load dữ liệu từ staging vào tabel student trong datawarehouse
	public void loadFromStaging() throws SQLException, NumberFormatException, ParseException {
		Statement stateStaging = connStaging.createStatement();
		String sqlStaging = "SELECT " + fields2 + " FROM " + tabelStaging;
		ResultSet rsStaging = stateStaging.executeQuery(sqlStaging);
		String sqlD = "INSERT INTO " + tableDW + fields + " values " + v;
		PreparedStatement preS = connDW.prepareStatement(sqlD);
		Statement stateDW = connDW.createStatement();
		// truy vấn staging
		while (rsStaging.next()) {
			// câu lệnh kiểm tra có lặp id hay không.( đáng lẽ không truyền số vô đâu, phải
			// lấy từ database config xem cái nào là xác định riêng cơ, nhưng mà làm xong
			// rồi mới nghĩ nên thôi, để lần sau sửa.
			String sql = "SELECT " + fields2 + " FROM " + tableDW + " WHERE " + tableDW + "." + listField.get(0) + "="
					+ rsStaging.getString(1) + " and date_expire = '9999-12-31'";
			ResultSet rsDW = stateDW.executeQuery(sql);
			// nếu mà không có id trùng nhau thì add vào và thực hiện vòng lặp kế tiếp
			if (!rsDW.next()) {
				System.out.println("load new data");
				setValuesDW(preS, rsStaging);
				preS.execute();
				continue;
			}
			// còn nếu trùng thì phải xét nó trùng hoàn toàn hay là update
			else {
				// nếu là update thì set hàng cũ date_expire bằng ngày hiện tại,add hàng mới vào.
				if (isUpdate(rsStaging, rsDW)) {
					System.out.println("update data");
					setExpireDate(rsStaging.getString(1)); // set ngày hiện tại
					setValuesDW(preS, rsStaging);// add hàng mới
					preS.execute();
					continue;
				} else {
					System.out.println(" duplicate !, do nothing");
				}

			}

		}

	}
	// Phương thức đảm nhận Load dữ liệu
	@SuppressWarnings("deprecation")
	private void setValuesDW(PreparedStatement preS, ResultSet rsStaging)
			throws NumberFormatException, SQLException, ParseException {
		for (int i = 0; i < listFieldFormat.size(); i++) {
			switch (listFieldFormat.get(i)) {
			case "int":
				preS.setInt(i + 1, Integer.parseInt(rsStaging.getString(i + 1)));
//				break;
				continue;
			case "nvarchar":
				preS.setString(i + 1, rsStaging.getString(i + 1));
//				break;
				continue;
			case "date":
				java.util.Date date1 = dateFormat.parse(rsStaging.getString(i + 1));
				System.out.println(date1);
//				preS.setDate(i + 1, new Date(date1.getYear(), date1.getMonth(), date1.getDay()));
				preS.setTimestamp(i+1, new Timestamp(date1.getTime()));
				continue;
			default:
				break;
			}
		}
// cái chổ ngày tháng làm tốn cả buổi
		preS.setTimestamp(listFieldFormat.size() + 1,
				java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(9999, 12, 31, 6, 6)));
//		preS.setTimestamp(listFieldFormat.size() + 1, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
//		preS.setTimestamp(listFieldFormat.size() + 1, new Timestamp(utilDate.getTime()));

	}
	// Bước 4: kiểm tra 
	private boolean isUpdate(ResultSet rsStaging, ResultSet rsDW) throws SQLException {
		List<String> listStaging = new LinkedList<String>();
		List<String> listDW = new LinkedList<String>();

		System.out.println(rsStaging.getString(1) + "\t" + rsStaging.getString(2) + "\t" + rsStaging.getString(3) + "\t"
				+ rsStaging.getString(4)+ "\t" + rsStaging.getString(5)+ "\t" + rsStaging.getString(6)+ "\t" + rsStaging.getString(7));
		System.out.println(
				rsDW.getString(1) + "\t" + rsDW.getString(2) + "\t" + rsDW.getString(3) + "\t" + rsDW.getString(4) + "\t" + rsDW.getString(5) + "\t" + rsDW.getString(6) + "\t" + rsDW.getString(7));
		for (int i = 0; i < listFieldFormat.size(); i++) {
			listStaging.add(rsStaging.getString(i + 1));
			switch (listFieldFormat.get(i)) {
			case "int":
				listDW.add(rsDW.getInt(i + 1) + "");
//				break;
				continue;
			case "nvarchar":
				listDW.add(rsDW.getString(i + 1));
//				break;
				continue;
			case "date":
				listDW.add(rsDW.getDate(i + 1) + "");
				continue;
			default:
				continue;
			}
		}
		//
		for (int i = 0; i < listStaging.size(); i++) {
			if (!listStaging.get(i).equalsIgnoreCase(listDW.get(i))) {
				System.out.println("values isnt equals " + listStaging.get(i) + " =!=== " + listDW.get(i));
				return true;
			}
		}

		return false;

	}

	private void setExpireDate(String id) throws SQLException {
		String sql = "UPDATE " + tableDW + " SET date_expire = CURDATE() WHERE id_student = " + id;
//		PreparedStatement pre = connDW.prepareStatement(sql);
//		pre.setString(1 , "GETDATE()");
//		pre.execute();
		Statement st = connDW.createStatement();
		st.execute(sql);

	}


	public static void main(String[] args) throws SQLException, NumberFormatException, ParseException {
		LoadingStaging ls = new LoadingStaging(1);
		ls.loadFromStaging();
	}

}
