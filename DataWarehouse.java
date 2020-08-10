package dw;
// step 3 int ETL , load data from starging into data warehouse

// định comment tiếng anh mà làm biếng nên thôi, dẹp đê!

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import mail.MailConfig;
import mail.SendMail;
import utils.ControlDB;
import utils.DBConnection;

public class DataWarehouse {
	Connection connStaging;
	Connection connDW;
	Connection connControll;
	int currIDFile; // chứa id file hiện tại đang thực hiện tranform và load
	String tableStaging; // chỉ ra tên của staging của data
	String tableDW;
	boolean isDim;
	String nkField;
	int idTarget;
	List<String> fields;
	List<String> formatFields;
	List<String> referenceDims;
	SimpleDateFormat dateFormat;
	java.util.Date utilDate;
	// chuẩn bị dữ liệu cho câu sql
	// insert
	private static final String TRANSFORM_SUCCESS = "TS";
	private static final String TRANSFORM_FAIL = "TF";
	ControlDB controlDB;

	public DataWarehouse() {
		connStaging = null;
		connDW = null;
		connControll = null;
		tableStaging = "";
		tableDW = "";
		nkField = "";
		fields = new ArrayList<String>();
		formatFields = new ArrayList<String>();
		referenceDims = new ArrayList<String>();
		// dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		utilDate = new java.util.Date();
		currIDFile = 0;
		controlDB = new ControlDB("controldb", "controldb", "log_file");
		// controlDB.loadDateDimToDW("date_dim");
	}

	/*
	 * Bước 1: Tạo DataWarehouse, Lấy các Connection tới
	 * Staging,Controldb,Datawarehouse ,gọi lại connection data warehouse từ
	 * connection của controldb
	 */
	public DataWarehouse(Connection conStaging, Connection connController) throws SQLException {
		this();
		this.connStaging = conStaging;
		this.connControll = connController;
		Statement state = connController.createStatement();
		// lấy thông tin data warehouse từ config
		String sql = "SELECT * FROM config_database WHERE config_database.stt = 2";
		ResultSet rs = state.executeQuery(sql);
		rs.next();
		// thiết lập connection tới data warehouse
		connDW = DBConnection.createConnectionWithURLLogin(rs.getString(2), rs.getString(3), rs.getString(4),
				rs.getString(5));
		connDW.setAutoCommit(false);
	}

	/*
	 * Bước 2 :Nhận vào ID file ( đã kiểm tra TR ở bước 2) của dữ liệu trong staging
	 */
	public void loadToDW(int idFile) throws NumberFormatException, ParseException, SQLException {

		try {
			// set id cho biết đang tiến hành load staing tương ứng của file nào
			currIDFile = idFile;
			/*
			 * bước 3 Lấy config id từ log qua connection controll
			 */
			Statement stateC = connControll.createStatement();
			String sqlC = "SELECT * FROM log_file WHERE data_file_id = " + idFile;
			ResultSet rsConfig = stateC.executeQuery(sqlC);
			rsConfig.next();
			int idConfig = (rsConfig.getInt("data_file_config_id"));
			/*
			 * Bước 4 Lấy Thông tin is DIim, Id target tabke từ configuration dựa vào config
			 * id
			 */
			String sqlF = "Select * From configuration where config_id = " + idConfig;
			ResultSet rsF = stateC.executeQuery(sqlF);
			rsF.next();
			// xác định là dim hay fact
			isDim = (rsF.getInt("is_Dim") == 1);
			System.out.println(" is dim : " + isDim);
			// xác định target table của nó là đâu
			idTarget = rsF.getInt("Id_Target_Table");
			String sql = "Select * from config_staging Where Id = " + idTarget;
			ResultSet reS = stateC.executeQuery(sql);
			reS.next();
			tableStaging = reS.getString("Name_Staging");
			tableDW = reS.getString("Name_Table_Target");
			// Bước 5: Tạo statement để kết nối với staging
			Statement smStaging = connStaging.createStatement();
			String sqlS = "SELECT  *  FROM " + tableStaging;
			ResultSet rsStaging = smStaging.executeQuery(sqlS);
			// là dim
			if (isDim) {
				// Bước 6A xác định NKField, Fields,FormatFields
				String sqlInfor = "Select * from config_dim where Name_Dim = N'" + tableDW + "'";
				ResultSet rsInfor = stateC.executeQuery(sqlInfor);
				rsInfor.next();
				nkField = rsInfor.getNString("NK_Field");
				fields.addAll(Arrays.asList(rsInfor.getNString("List_Fields").split(",")));
				formatFields.addAll(Arrays.asList(rsInfor.getNString("List_Formats").split(",")));
				// Bước 7A load to dw
				loadToDim(rsStaging);
				System.out.println("Load to dim success");
				// Bước 9
				controlDB.updateLogAfterLoadingIntoDW(currIDFile, DataWarehouse.TRANSFORM_SUCCESS);
				connDW.commit();
			}
			// data fact
			if (!isDim) {
				// Bước 6B: xác định nk field, reference dims
				String sqlInfor = "Select * from config_fact where Name_Fact =  N'" + tableDW + "'";
				ResultSet rsInfor = stateC.executeQuery(sqlInfor);
				rsInfor.next();
				nkField = rsInfor.getNString("NK_Field");
				referenceDims.addAll(Arrays.asList(rsInfor.getNString("Reference_Dims").split(",")));
				// Bước 7B load to fact
				loadToFact(rsStaging);
				System.out.println("Load to fact success");
				// Bước 9
				controlDB.updateLogAfterLoadingIntoDW(currIDFile, DataWarehouse.TRANSFORM_SUCCESS);
				connDW.commit();

			}

		} catch (SQLException e) {
			e.printStackTrace();
			connDW.rollback();
			System.out.println("load to dw err");
			controlDB.updateLogAfterLoadingIntoDW(currIDFile, DataWarehouse.TRANSFORM_FAIL);
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, "Load to Datawarehouse fail , File id:" + idFile,
					e.getMessage());
		}

		/*
		 * Bước 10:clear Fields,FormatFields,referenceDims
		 */
		fields.clear();
		formatFields.clear();
		referenceDims.clear();

	}

	/*
	 * Bước 7B:load vào fact ( làm việc với từng dòng dữ liệu)
	 */
	private void loadToFact(ResultSet rsStaging) throws SQLException, NumberFormatException, ParseException {

		String[] arrSQL = getFieldFactMasks();
		String sqlFact = "INSERT INTO " + tableDW + arrSQL[0] + "values" + arrSQL[1];
		PreparedStatement prFact = connDW.prepareStatement(sqlFact);

		while (rsStaging.next()) {
			String sqlCheck = "Select  * from " + tableDW + " where " + nkField + " = '" + rsStaging.getString(nkField)
					+ "'";
			// đề phòng trường hợp dòng rỗng khi hêt dữ liệu !
			if (rsStaging.getString(nkField).isEmpty() || rsStaging.getString(nkField) == "") {
				System.out.println("rong");
				continue;
			}
			// Kiểm tra dữ liệu có tồn tại trước đó hay không
			Statement stCheck = connDW.createStatement();
			ResultSet rsCheck = stCheck.executeQuery(sqlCheck);
			if (!rsCheck.next()) {
				System.out.println("load new fact");
				// Bước 8B( load 1 dòng mới)
				setValuesFact(rsStaging, prFact);
				prFact.execute();
			}
			// có dữ liệu rồi thì bỏ qua

		}

	}

	/*
	 * Bước 8B( load 1 dòng mới) SetValuesFact , thêm id file vào cho dễ truy xuất
	 * sau này
	 */
	private boolean setValuesFact(ResultSet rsStaging, PreparedStatement prFact) throws SQLException, ParseException {
		// add phần tử tương ứng với NK_Field
		prFact.setNString(1, rsStaging.getNString(nkField));
		for (int i = 0; i < referenceDims.size(); i++) {
			if (!referenceDims.get(i).equalsIgnoreCase("date_dim")) {
				prFact.setNString((i + 1) + 1, rsStaging.getNString((i + 1) + 2));
				continue;
			} else { // là date dim
				String date = rsStaging.getString((i + 1) + 2);
				if (Pattern.matches("^(0[1-9]|1[012])[-/.](0[1-9]|[12][0-9]|3[01])[-/.](19|20)\\d\\d$", date)
						|| Pattern.matches("^(19|20)\\d\\d[-/.](0[1-9]|[12][0-9]|3[01])[-/.](0[1-9]|1[012])$", date)) {
//					DateFormat df = new SimpleDateFormat("yyyy/mm/dd");
//					java.util.Date date1 = df.parse(date);

					SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MM/yyyy");
					SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
					String ds2 = sdf2.format(sdf1.parse(date));
					String sqlDate = "Select date_sk from date_dim where full_date ='" + ds2 + "'";
					ResultSet rs = connDW.createStatement().executeQuery(sqlDate);
					rs.next();
					prFact.setString((i + 1) + 1, rs.getString("date_sk"));
				} else {
					System.out.println("sai định dạng ");
					return false;
				}
			}
		}
		// add giá trị ID file
		prFact.setInt(referenceDims.size() + 2, currIDFile);
		return true;
	}

	// hỗ trợ tạo câu truy vấn cho load to fact
	private String[] getFieldFactMasks() throws SQLException {
		// thêm cái nk field so với Dim
		String f = " (" + nkField + ",";
		String v = " (?,";
		// lấy nhãnh các trường NK
		String nk = "select Name_sk_Dims from config_fact where Name_Fact = '" + tableDW + "'";
		Statement stm = connControll.createStatement();
		ResultSet rs = stm.executeQuery(nk);
		rs.next();
		String[] nameSKDims = rs.getNString("Name_sk_Dims").split(",");

		for (String s : nameSKDims) {
			f += s + ",";
			v += "?,";
		}
		// thêm trường Id_File
		f += "File_ID) ";
		v += "?) ";
		String[] result = { f, v };
		return result;
	}

	/*
	 * Bước 7A load vào dim ,thực hiện load data từng dòng staging vào dim
	 */
	private void loadToDim(ResultSet rsStaging) throws SQLException, NumberFormatException, ParseException {

		String[] mark = getFieldDimMasks();
		String sqlDim = "insert into " + tableDW + mark[0] + " values " + mark[1];
		PreparedStatement prDim = connDW.prepareStatement(sqlDim);
		boolean checkNext = false;
		while (checkNext = rsStaging.next()) {
			// kiểm tra mới, trùng hay update
			System.out.println(" in rsStaging: " + rsStaging.getString(1) + "\t" + rsStaging.getString(2) + "\t"
					+ rsStaging.getString(3) + "\t");
			String sqlCheck = "Select  * from " + tableDW + " where " + nkField + " = '" + rsStaging.getString(nkField)
					+ "' and date_expire = '9999/12/30'";
			// phòng việc staging bị dòng rỗng
			if (rsStaging.getString(nkField).isEmpty()) {
				System.out.println("rong");
				continue;
			}
			Statement stCheck = connDW.createStatement();
			ResultSet rsCheck = stCheck.executeQuery(sqlCheck);
			// chưa có dữ liệu, load new
			if (!rsCheck.next()) {
				System.out.println("load new ");
				// bước 8A.1:(load 1 dòng mới) setValuesDim
				if (setValuesDim(rsStaging, prDim))
					prDim.execute();
			} else {
				// đã có dữ liệu -> kiểm tra có phải update hay không
				if (isUpdate(rsStaging, rsCheck)) {
					System.out.println(" đã có dữ liệu, thuộc update");
					/*
					 * Bước 8A.2( set dòng cũ hết hạn) setExpireDate
					 */
					setExpireDate(rsStaging.getString(nkField));
					if (setValuesDim(rsStaging, prDim))
						prDim.execute();
					continue;
				} else {
					System.out.println(" duplicate !, do nothing");
				}
			}
		}

	}

	/*
	 * bước 8A.1:(load 1 dòng mới) setValuesDim
	 * 
	 */
	private boolean setValuesDim(ResultSet rsStaging, PreparedStatement prDim)
			throws NumberFormatException, SQLException, ParseException {
		for (int i = 0; i < formatFields.size(); i++) {
			switch (formatFields.get(i)) {
			case "int":
				prDim.setInt(i + 1, Integer.parseInt(rsStaging.getNString(fields.get(i))));
				continue;
			case "varchar":
				prDim.setNString(i + 1, rsStaging.getNString(fields.get(i)));
				continue;
			case "date":
				String date = rsStaging.getString(fields.get(i));
				if (Pattern.matches("^(0[1-9]|1[012])[-/.](0[1-9]|[12][0-9]|3[01])[-/.](19|20)\\d\\d$", date)
						|| Pattern.matches("^(19|20)\\d\\d[-/.](0[1-9]|[12][0-9]|3[01])[-/.](0[1-9]|1[012])$", date)) {
					java.util.Date date1 = dateFormat.parse(date);
					// prDim.setString(i + 1, date);
					prDim.setTimestamp(i + 1, new Timestamp(date1.getTime()));
					continue;
				} else {
					return false;
				}
			default:
				break;
			}

		}
		// set date_expire
		prDim.setTimestamp(formatFields.size() + 1,
				java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(9999, 12, 30, 6, 6)));
		return true;
	}

	// hỗ trợ xây dựng câu truy vấn của fact
	private String[] getFieldDimMasks() {
		String f = " (";
		String v = " (";
		for (String s : fields) {
			f += s + ",";
			v += "?,";
		}
		// thêm trường date_expore
		f += "Date_expire) ";
		v += "?) ";
		String[] result = { f, v };
		return result;
	}

	/*
	 * kiểm tra có phải update hay không
	 * 
	 */
	private boolean isUpdate(ResultSet rsStaging, ResultSet rsCheck) throws SQLException {
		// cgir có 1 dòng dữ liệu thôi
		List<String> listStaging = new LinkedList<String>();
		List<String> listDW = new LinkedList<String>();
		for (int i = 0; i < formatFields.size(); i++) {
			listStaging.add(rsStaging.getString(i + 1));
			// resutset staging bắt đầu từ 1 còn cái rsCheck vì nó lấy dư 1
			// trường sk nên +1 => = +2
			switch (formatFields.get(i)) {
			case "int":
				listDW.add(rsCheck.getInt(i + 2) + "");
				continue;
			case "varchar":
				listDW.add(rsCheck.getString(i + 2));
				continue;
			case "date":
				listDW.add(rsCheck.getDate(i + 2) + "");
				continue;
			default:
				continue;
			}
		}
		for (int i = 0; i < listStaging.size(); i++) {
			if (!listStaging.get(i).equalsIgnoreCase(listDW.get(i))) {
				System.out.println("values isnt equals " + listStaging.get(i) + " =!=== " + listDW.get(i));
				return true;
			}
		}

		return false;

	}

	/*
	 * Bước 8A.2( set dòng cũ hết hạn) setExpireDate
	 */
	private void setExpireDate(String id) throws SQLException {
		String sql = "UPDATE " + tableDW + " SET date_expire = Date('2013-06-20') " + "  WHERE  " + nkField + "  = "
				+ id;
		Statement st = connDW.createStatement();
		st.execute(sql);

	}

	public static void main(String[] args) throws SQLException, NumberFormatException, ParseException {
		Connection cS = DBConnection.createConnection("stagingdb");
		Connection cC = DBConnection.createConnection("controldb");
		DataWarehouse dw = new DataWarehouse(cS, cC);
		// ControlDB controlDB = new ControlDB("controldb", "controldb", "log_file");
		// controlDB.loadDateDimToDW("date_dim");
		// dw.loadToDW(9);
//		System.out.println(
//				Pattern.matches("^(0[1-9]|1[012])[-/.](0[1-9]|[12][0-9]|3[01])[-/.](19|20)\\d\\d$", "30/6/2020"));
//		System.out.println(
//				Pattern.matches("^(19|20)\\d\\d[-/.](0[1-9]|[12][0-9]|3[01])[-/.](0[1-9]|1[012])$", "30/6/2020"));
//		System.out.println(
//				Pattern.matches("^[0-3]?[0-9].[0-3]?[0-9].(?:[0-9]{2})?[0-9]{2}$", "30/6/2020"));

	}

}
