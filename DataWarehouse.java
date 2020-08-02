package dw;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.poi.sl.usermodel.PaintStyle.SolidPaint;

import utils.ControlDB;
import utils.DBConnection;

public class DataWarehouse {
	Connection connStaging;
	Connection connDW;
	Connection connControll;
	int currIDFile; // chứa id file hiện tại đang thực hiện tranform và load
	String tableStaging; // chỉ ra tên của stagin g của data
	String tableDW;
	boolean isDim;
	String nkField;
	int idTarget;
	List<String> fields;
	List<String> formatFields;
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
//		dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		utilDate = new java.util.Date();
		currIDFile = 0;
		controlDB = new ControlDB("controldb", "controldb", "log_file");
	}

	// Bước 1: tạo mới DW , sử dụng lại connection staging và connection config
	// trước đó. gọi lại connection data warehouse từ config
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

	// Bước 2 thực hiện load dữ liệu từ file theo id mà bên staging đưa ( file
	// này
	// đã xác nhận trạng thái ER trên log )
	public void loadToDW(int idFile) throws NumberFormatException, ParseException, SQLException {

		try {
			// set id cho biết đang tiến hành load staing tương ứng của file nào
			currIDFile = idFile;
			// kết nối với controll để lấy dữ liệu file chuẩn bị query
			// prepareQuery(idFile);
			Statement stateC = connControll.createStatement();
			String sqlC = "SELECT * FROM log_file WHERE data_file_id = " + idFile;
			ResultSet rsConfig = stateC.executeQuery(sqlC);
			rsConfig.next();
			int idConfig = (rsConfig.getInt("data_file_config_id"));
			String sqlF = "Select * From configuration where config_id = " + idConfig;
			ResultSet rsF = stateC.executeQuery(sqlF);
			rsF.next();

			isDim = (rsF.getInt("is_Dim") == 1); // xác định dữ liệu là dim
													// or fact
			System.out.println( " is dim : " + isDim);
			idTarget = rsF.getInt("Id_Target_Table"); // xác định target
														// table của nó là
														// đâu
			String sql = "Select * from config_staging Where Id = " + idTarget;
			ResultSet reS = stateC.executeQuery(sql);
			reS.next();
			tableStaging = reS.getString("Name_Staging");
			tableDW = reS.getString("Name_Table_Target");
			if (isDim) {
				String sqlInfor = "Select * from config_dim where Name_Dim = N'" + tableDW + "'";
				ResultSet rsInfor = stateC.executeQuery(sqlInfor);
				rsInfor.next();
				nkField = rsInfor.getNString("NK_Field");
				fields.addAll(Arrays.asList(rsInfor.getNString("List_Fields").split(",")));
				formatFields.addAll(Arrays.asList(rsInfor.getNString("List_Formats").split(",")));
			}
			// if (!isDim) { // data fact
			// String sqlInfor = "Select * from config_fact where Name_Fact =
			// N'" + tableDW + "'";
			// ResultSet rsInfor = stateC.executeQuery(sqlInfor);
			// rsInfor.next();
			// nkField = rsInfor.getNString("NK_Field");
			// fields.addAll(Arrays.asList(rsInfor.getNString("List_Fields").split(",")));
			// formatFields.addAll(Arrays.asList(rsInfor.getNString("List_Formats").split(",")));
			// }
			// tạo statement config
			// tạo statement để kết nối với staging
			Statement smStaging = connStaging.createStatement();
			String sqlS = "SELECT  *  FROM " + tableStaging;
			ResultSet rsStaging = smStaging.executeQuery(sqlS);
			if (isDim) {
				loadToDim(rsStaging);
//				System.out.println("Sf/segsdsd");
//				System.out.println(currIDFile + "UUUUU");
				// controlDB.updateLogAfterLoadingIntoDW(2,"UU");
				System.out.println("Load to dim success");
				controlDB.updateLogAfterLoadingIntoDW(currIDFile, DataWarehouse.TRANSFORM_SUCCESS);
				connDW.commit();
				fields.clear();
				formatFields.clear();
				//truncateStaging();

			}
			// System.out.println(" load to dim xong");
			// loadToFact(rsStaging);
			// }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			connDW.rollback();
//			System.out.println(currIDFile + "AAA");
			System.out.println("load to dim err");
			controlDB.updateLogAfterLoadingIntoDW(currIDFile, DataWarehouse.TRANSFORM_FAIL);
//			System.out.println("Lỗi");
		}

	}

	// load fact sau khi loà dim xong, lấy các trường mà dim của nó ( các
	// reference
	// dim ) cho là natural key. còn các measurements thì chưa rõ nên mình chưa
	// làm
	private void loadToFact(ResultSet rsStaging) throws SQLException, NumberFormatException, ParseException {
		Statement smControll = connControll.createStatement();
		String sqlconfigFact = "SELECT * FROM config_fact ";
		ResultSet rsConfigFact = smControll.executeQuery(sqlconfigFact);
		// lấy các giá trị config
		String nameFact = "";
		List<String> tableReferences = new ArrayList<String>();
		// measurement hiện h vẫn chưa đụng chạm nên mình coi như nó là danh
		// sách các
		// trường có giá trị int.
		List<String> measurements = new ArrayList<String>();
		List<String> listNKReference = new ArrayList<String>();
		while (rsConfigFact.next()) {
			// lấy thông tin bảng Dim
			nameFact = rsConfigFact.getNString("Table_name");
			tableReferences.addAll(Arrays.asList(rsConfigFact.getNString("Table_references").split(",")));

			// measurements.addAll(Arrays.asList(rsConfigFact.getNString("Measurements").split(",")));
			String[] measuremants = rsConfigFact.getNString("Measurements").split(",");

			// từng fact kết nối với từng config dim của nó để lấy các tên
			// trường Natural
			// Key
			for (String dimName : tableReferences) {
				Statement smConfigDim = connControll.createStatement();
				String sqlConfigDim = "SELECT * FROM config_dim WHERE Table_name = " + dimName;
				ResultSet rsDim = smConfigDim.executeQuery(sqlConfigDim);
				// hiện h mình coi như là natural key chỉ có 1 cho mỗi dim (
				// không phải
				// composite key)
				rsDim.next();
				// add nk đó vào danh sách các NK
				listNKReference.add(rsDim.getNString(1));
			}
			// insert vào fact theo các tên NK
			String[] arrSQL = getFieldFactMasks(listNKReference, measurements);
			String sqlFact = "INSERT INTO " + nameFact + arrSQL[0] + "values" + arrSQL[1];
			PreparedStatement prFact = connDW.prepareStatement(sqlFact);
			setValuesFact(rsStaging, prFact, listNKReference);
		}

	}

	private void setValuesFact(ResultSet rsStaging, PreparedStatement prFact, List<String> listNKReference) {
		// TODO Auto-generated method stub

	}

	// methold này còn chưa hoàn thiện vì cái measurement
	private String[] getFieldFactMasks(List<String> listNKReference, List<String> measurements) {
		String f = " (";
		String v = " (";
		// lấy nhãnh các trường NK
		for (String s : listNKReference) {
			f += s + ",";
			v += "?,";
		}
		// measurement chưa làm
		for (String s : measurements) {
			// TODO some thing
		}
		//
		// thêm trường Id_File
		f += "ID_File) ";
		v += "?) ";
		String[] result = { f, v };
		return result;
	}

	// thực hiện load data từng dòng staging vào dim
	private void loadToDim(ResultSet rsStaging) throws SQLException, NumberFormatException, ParseException {
		// kiểm tra is update hay insert new
		String[] mark = getFieldDimMasks();
		String sqlDim = "insert into " + tableDW + mark[0] + " values " + mark[1];
		PreparedStatement prDim = connDW.prepareStatement(sqlDim);
		while (rsStaging.next()) {
			// kiểm tra mới, trùng hay update
			String sqlCheck = "Select  * from " + tableDW + " where " + nkField + " = " + rsStaging.getString(nkField)
					+ " and date_expire = '9999-12-31'";
			// System.out.println(sqlCheck);
			Statement stCheck = connDW.createStatement();
//			System.out.println(sqlCheck);
			ResultSet rsCheck = stCheck.executeQuery(sqlCheck);
			// while(rsCheck.next()) {
			// System.out.println("giá trị trùng :" + rsCheck.getString(1)+
			// rsCheck.getString(2)+"\t"+ rsCheck.getString(3)+"\t"+
			// rsCheck.getString(4)+"\t");
			// }
			if (!rsCheck.next()) { // chưa có dữ liệu, load new
				System.out.println("load new ");

				setValuesDim(rsStaging, prDim);
				prDim.execute();
				continue;
			} else {
				// đã có dữ liệu -> kiểm tra có phải update hay không
				if (isUpdate(rsStaging, rsCheck)) {
					System.out.println(" đã có dữ liệu, thuộc update");
					setExpireDate(rsStaging.getString(nkField)); // set ngày
																	// hiện tại
					setValuesDim(rsStaging, prDim);
					prDim.execute();
					continue;
				} else {
					System.out.println(" duplicate !, do nothing");
				}
			}
		}

		// rsStaging bây h coi như là 1 dòng dữ liệu, không.next ở đoạn này

		// reset lại 2 cái list và tên
		// fields.clear();
		// formatFields.clear();

	}

	// load dữ liệu vào Dim ( bỏ vào prepared statement )
	private void setValuesDim(ResultSet rsStaging, PreparedStatement prDim)
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
				java.util.Date date1 = dateFormat.parse(rsStaging.getNString(fields.get(i)));
				prDim.setTimestamp(i + 1, new Timestamp(date1.getTime()));
				continue;
			default:
				break;
			}
		}
		// set date_expire
		prDim.setTimestamp(formatFields.size() + 1,
				java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(9999, 12, 31, 6, 6)));
	}

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

	private void prepareQuery(int idFile) throws SQLException {
		Statement stateC = connControll.createStatement();
		String sqlC = "SELECT * FROM configuration WHERE config_id = " + idFile;
		ResultSet rsConfig = stateC.executeQuery(sqlC);
		rsConfig.next();
		isDim = (rsConfig.getInt("is_Dim") == 1); // xác định dữ liệu là dim or
													// fact
		idTarget = rsConfig.getInt("Id_Target_Table"); // xác định target table
														// của nó là đâu
		String sql = "Select * from config_staging Where Id = " + idTarget;
		ResultSet reS = stateC.executeQuery(sql);
		reS.next();
		tableStaging = reS.getString("Name_Staging");
		tableDW = reS.getString("Name_Table_Target");
		if (isDim) {
			String sqlInfor = "Select * from config_dim where Name_Dim = N'" + tableDW + "'";
			ResultSet rsInfor = stateC.executeQuery(sqlInfor);
			rsInfor.next();
			nkField = rsInfor.getNString("NK_Field");
			fields.addAll(Arrays.asList(rsInfor.getNString("List_Fields").split(",")));
			formatFields.addAll(Arrays.asList(rsInfor.getNString("List_Formats").split(",")));
		}
	}

	private boolean isUpdate(ResultSet rsStaging, ResultSet rsCheck) throws SQLException {
		// cgir có 1 dòng dữ liệu thôi
		System.out.println("size formatFields : "+ formatFields.size());
		List<String> listStaging = new LinkedList<String>();
		List<String> listDW = new LinkedList<String>();
		for (int i = 0; i < formatFields.size(); i++) {
			listStaging.add(rsStaging.getString(i + 1)); 
//			 resutset staging bắt đầu từ 1 còn cái rsCheck vì nó lấy dư 1 trường sk nên +1 => =  +2
			switch (formatFields.get(i)) {
			case "int":
				listDW.add(rsCheck.getInt(i + 2) + "");
				// break;
				continue;
			case "varchar":
				listDW.add(rsCheck.getString(i + 2));
				// break;
				continue;
			case "date":
				listDW.add(rsCheck.getDate(i + 2) + "");
				// listDW.add(rsCheck.getTimestamp(i +2) +"");
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

	private void setExpireDate(String id) throws SQLException {
		String sql = "UPDATE " + tableDW + " SET date_expire = CURDATE() " + "  WHERE  " + nkField + "  = " + id;
		Statement st = connDW.createStatement();
		st.execute(sql);

	}

	private void truncateStaging() throws SQLException {
		String sqlTruncate = "TRUNCATE " + tableStaging;
		connStaging.createStatement().execute(sqlTruncate);

	}

	public static void main(String[] args) throws SQLException, NumberFormatException, ParseException {
		Connection cS = DBConnection.createConnection("stagingdb");
		Connection cC = DBConnection.createConnection("controldb");
		DataWarehouse dw = new DataWarehouse(cS, cC);
		dw.loadToDW(1);

	}

}
