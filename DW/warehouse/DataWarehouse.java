package warehouse;
// step 3 int ETL , load data from starging into data warehouse
// định comment tiếng anh mà làm biếng nên thôi, dẹp đê!
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import mail.SendMail;
import utils.DBConnection;

public class DataWarehouse {
	Connection connStaging;
	Connection connDW;
	Connection connControll;
	int currIDFile; // chứa id file hiện tại đang thực hiện tranform và load
	List<String> listField;
	List<String> listFieldFormat;
	String tableSDW; // là tabel chứa data trong staging , bên data warehouse cũng sử dụng lại cái
						// tên này
	SimpleDateFormat dateFormat;
	java.util.Date utilDate;
	// chuẩn bị dữ liệu cho câu sql
	String colums;// danh sách các trường, chưa cộng cột id_file với cột date_expire dùng cho câu
					// select - staging (lấy từ bảng config và không chứa stt)
	String v;// ? cho prepareStatement
	String colums2;// danh sách các trường đã cộng côt id_file với cột data_expire dùng cho câu
					// insert
	private static final String TRANSFORM_SUCCESS = "TS";
	private static final String TRANSFORM_FAIL = "TF";

	public DataWarehouse() {
		connStaging = null;
		connDW = null;
		connControll = null;
		listField = new LinkedList<String>();
		listFieldFormat = new LinkedList<String>();
		tableSDW = "";
		dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		utilDate = new java.util.Date();
		colums = "";
		colums2 = "";
		v = "";
		currIDFile = 0;
	}

	// Bước 1: tạo mới DW , sử dụng lại connection staging và connection config
	// trước đó. gọi lại connection data warehouse từ config
	public DataWarehouse(Connection conStaging, Connection connController) throws SQLException {
		this();
		this.connStaging = conStaging;
		this.connControll = connController;
		Statement state = connController.createStatement();
		// lấy thông tin data warehouse từ config
		String sql = "SELECT * FROM config_database WHERE config_database.stt = 2"; // 2 là dòng chúa thông tin về data
																					// warehouse
		ResultSet rs = state.executeQuery(sql);
		rs.next();
		// thiết lập connection tới data warehouse
		connDW = DBConnection.createConnectionWithURLLogin(rs.getString(2), rs.getString(3), rs.getString(4),
				rs.getString(5));
		System.out.println(rs.getString(5)+"VVV");
	}

	// Bước 2 thực hiện load dữ liệu từ file theo id mà bên staging đưa ( file này
	// đã đựa staging xác định trạng thái ER trên log)
	public void loadToDW(int idFile) {

		try {

			// tạo statement để lấy thông tin file mà stagingđang chứa
			currIDFile = idFile; // set id cho biết đang tiến hành load staing tương ứng của file nào
			Statement stateC = connControll.createStatement();
			String sqlC = "SELECT * FROM configuration WHERE config_id = " + idFile;
			System.out.println();
			ResultSet rsConfig = stateC.executeQuery(sqlC);
			rsConfig.next();
			// chuẩn bị các biến cho câu query
			prepareQuery(rsConfig);
			// tạo statement để kết nối với staging
			Statement stateS = connStaging.createStatement();
			String sqlS = "SELECT  " + colums + " FROM " + tableSDW;
			// tạo statement kết nối với data warehouse
			String sqlD = "INSERT INTO " + tableSDW + " " + colums2 + " values " + v;
			PreparedStatement preS = connDW.prepareStatement(sqlD);
			Statement stateDW = connDW.createStatement();
			// executeQuery staing
			ResultSet rsStaging = stateS.executeQuery(sqlS);
			while (rsStaging.next()) {
				// Kiểm tra có lặp id hay không, ta mặc định id là cột đầu tiên.
				String sql = "SELECT " + colums + " FROM " + tableSDW + " WHERE " + tableSDW + "." + listField.get(0)
						+ "=" + rsStaging.getString(1) + " and date_expire = '9999-12-31'";
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
					// nếu là update thì set hàng cũ date_expire bằng ngày hiện tại,add hàng mới
					// vào.
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
			// đã xong transform - load vào data ware xong, không lỗi thì :
			executeSuccess();

		} catch (Exception e) {
			try {
				executeFail();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	private void prepareQuery(ResultSet rsConfig) throws SQLException {
		tableSDW = rsConfig.getString(3); // đặt giá tabel trong data staging mà data warehouse sẽ lấy
		String[] columSplit = rsConfig.getString(8).split(",");
		colums = "(";
		v = "(";
		for (String s : columSplit) {
			colums += s + ",";
			v += "?,";
			listField.add(s);
		}
		colums2 = colums + "date_expire,id_file)";
		colums = colums.substring(1, colums.length() - 1); // loại bỏ dấu ',' dư thừa cuói
//		colums += ")";
		v += "?,?)";
		// lấy format của các colum
		String[] columFormatSplit = rsConfig.getString(9).split(",");
		for (String s : columFormatSplit) {
			listFieldFormat.add(s);
		}
	}

	// Phương thức đảm nhận Load dữ liệu
	private void setValuesDW(PreparedStatement preS, ResultSet rsStaging)
			throws NumberFormatException, SQLException, ParseException {
		for (int i = 0; i < listFieldFormat.size(); i++) {
			switch (listFieldFormat.get(i)) {
			case "int":
				preS.setInt(i + 1, Integer.parseInt(rsStaging.getString(i + 1)));
//				break;
				continue;
			case "varchar":
				preS.setString(i + 1, rsStaging.getString(i + 1));
//				break;
				continue;
			case "date":
				java.util.Date date1 = dateFormat.parse(rsStaging.getString(i + 1));
				System.out.println(date1);
//				preS.setDate(i + 1, new Date(date1.getYear(), date1.getMonth(), date1.getDay()));
				preS.setTimestamp(i + 1, new Timestamp(date1.getTime()));
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
		// set id file
		preS.setInt(listFieldFormat.size() + 2, currIDFile);
	}

	// Bước 3: kiểm tra
	private boolean isUpdate(ResultSet rsStaging, ResultSet rsDW) throws SQLException {
		List<String> listStaging = new LinkedList<String>();
		List<String> listDW = new LinkedList<String>();

//		System.out.println(rsStaging.getString(1) + "\t" + rsStaging.getString(2) + "\t" + rsStaging.getString(3) + "\t"
//				+ rsStaging.getString(4) + "\t" + rsStaging.getString(5) + "\t" + rsStaging.getString(6) + "\t"
//				+ rsStaging.getString(7));
//		System.out.println(rsDW.getString(1) + "\t" + rsDW.getString(2) + "\t" + rsDW.getString(3) + "\t"
//				+ rsDW.getString(4) + "\t" + rsDW.getString(5) + "\t" + rsDW.getString(6) + "\t" + rsDW.getString(7));
		for (int i = 0; i < listFieldFormat.size(); i++) {
			listStaging.add(rsStaging.getString(i + 1));
			switch (listFieldFormat.get(i)) {
			case "int":
				listDW.add(rsDW.getInt(i + 1) + "");
//				break;
				continue;
			case "varchar":
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
		String sql = "UPDATE " + tableSDW + " SET date_expire = CURDATE(), id_file = " + currIDFile
				+ "  WHERE id_student = " + id;
		Statement st = connDW.createStatement();
		st.execute(sql);

	}
	private void executeSuccess() throws SQLException {
		// update status file trong log thành TS
		updateStatus(TRANSFORM_SUCCESS);
		// truncate Staging
		truncateStaging();
	}
	private void executeFail() throws SQLException {
		// update status file trong log thành TF
		updateStatus(TRANSFORM_FAIL);
		// truncate Staging
		truncateStaging();
		// sent mail
		SendMail.sendMail("adminDW_NLS@gmail.com", "TRANSFROM ERR", " can't transform file hava id " + currIDFile);
		// delete all row have this id_file in data warehouse
		deleteDataWithIdFile();
	}
	// cập nhật status của file
	private void updateStatus(String status) throws SQLException {
		String sql = "UPDATE log_file SET file_status = " + status;
		Statement state = connControll.createStatement();
		state.execute(sql);
	}

	// truncate staging, chuẩn bị sẵn sàng cho dữ liệu mới.
	private void truncateStaging() throws SQLException {
		Statement state = connControll.createStatement();
		// lấy thông tin data warehouse từ config
		ResultSet rs = state.executeQuery("SELECT database_name FROM config_database WHERE config_database.stt = 1");
		rs.next();
		Statement stateS = connStaging.createStatement();
		stateS.execute("TRUNCATE " + rs.getString(0) + "." + tableSDW);
	}

	private void deleteDataWithIdFile() throws SQLException {
		Statement st = connDW.createStatement();
		String sql = "DELETE FROM " + tableSDW + " WHERE last_name = " + currIDFile;
		st.execute(sql);

	}

	public static void main(String[] args) throws SQLException, NumberFormatException, ParseException {
		Connection cS = DBConnection.createConnection("stagingdb");
		Connection cC = DBConnection.createConnection("controldb");
		System.out.println(cS);
		System.out.println(cC);
		DataWarehouse dw = new DataWarehouse(cS, cC);
		dw.loadToDW(1);

	}

}
