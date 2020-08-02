package etl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.zip.ZipException;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import control.Configuration;
import control.Log_file;
import control.SCP_DownLoad;
import download.DownloadPSCP;
import download.PSCPProcess;
import dw.DataWarehouse;
import mail.MailConfig;
import mail.SendMail;
import utils.ControlDB;
import utils.DBConnection;

public class Staging {
	private DownloadPSCP pscp=null;
	Connection cS = DBConnection.createConnection("stagingdb");
	Connection cC = DBConnection.createConnection("controldb");
	private Scanner sc = new Scanner(System.in);
	private ControlDB controlDb=new ControlDB("controldb","stagingdb", "configuration");
	private DataWarehouse dw=new DataWarehouse(cS, cC);
	private DataProcess dp=new DataProcess();
	
	// Trạng thái khi dữ liệu file đã download về, cập nhật vào log_file là ER=> sẵn sàng load vào staging
	private static final String FILE_STATUS_READY="ER";
	// Trạng thái khi dữ liệu file đã đưa vào staging và cập nhật vào log_file là TR=> sẵn sàng transform qua warehouse
	private static final String FILE_STATUS_TRANSFORM="TR";
	// Trạng thái khi dữ liệu file đã đưa vào staging, nhưng bị lỗi và cập nhật vào log_file là ERRO
	private static  final String FILE_STATUS_ERRO="ERRO";
	// Khai báo tên bảng log_file
	private static final String TABLE_LOG_FILE = "log_file";
	// Khai báo biến để kiểm tra dữ liệu đã vào staging hay chưa?
	private boolean isLoadedToStaging= false;
	
	public Staging() throws SQLException{
		dp.setControlDb(controlDb);
		this.pscp=new DownloadPSCP();
		
	}
	// Phương thức lấy thời gian hiện tại.
	public String getCurrentTime() {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		return dtf.format(now);
	}

	// Phương thức này để cập nhật trạng thái trong log_file khi file đã download về.
	public void loadFileStatus(DataProcess dp, String table_name, int id_config) throws SQLException{
	       // Lấy thời gian hiện tại.
	       	String timestamp= getCurrentTime();
	       // Lấy tất cả dữ liệu các trường configuration theo id_config 
			Configuration configuration=dp.getControlDb().selectAllFieldConfigurationByConfigId((id_config));
			String import_dir = configuration.getImport_dir();
			int config_id = configuration.getConfig_id();
			//Lấy tất cả tên của các file đã ghi vào log_file
			ArrayList<String> listFileNameCurrent=dp.getControlDb().selectAllFileNameInLogFile("log_file");

			File imp_dir = new File(import_dir);
			if (imp_dir.exists()) {
				File[] listFile = imp_dir.listFiles();
				if(listFile.length>0){
				for(File f: listFile){
					if(listFileNameCurrent.size()>0){
						// Nếu danh sách tên file hiện tại trong log mà không chứa tên file ta muốn ghi log thì tiến hành ghi log.
						if(!listFileNameCurrent.contains(f.getName())){
							dp.getControlDb().insertLogFileStatus(table_name, f.getName(), config_id, FILE_STATUS_READY,timestamp);
					}
				}
					else{
						dp.getControlDb().insertLogFileStatus(table_name, f.getName(), config_id, FILE_STATUS_READY,timestamp);
					}
				}
				
				SendMail.sendMail(MailConfig.EMAIL_RECEIVER, "URGENT FILE INFORMATION",
						"Load file status successfully!");
				System.out.println("Load file status successfully!");
			}
				else{
					SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE,
							"No any file here to load file status!");
					System.out.println("No any file here to load file status!");
				}
				
			}
			else{
				System.out.println("Path not exists!!!");
				return;
			}
			
		}
		
	
	public boolean extractToStagingDB(DataProcess dp, int id_config) throws SQLException, NumberFormatException, ParseException{
		// Lấy dữ liệu các trường từ bảng configuration theo id_config 
		Configuration configuration = dp.getControlDb().selectAllFieldConfigurationByConfigId(id_config);
		System.out.println(configuration.toString());
		String target_table = configuration.getTarget_table();
		String import_dir = configuration.getImport_dir();
		String success_dir= configuration.getSuccess_dir();
		String error_dir = configuration.getError_dir();
		String delim = configuration.getDelimmiter();
		String column_list =  configuration.getColumn_list();
		StringTokenizer strToken= new StringTokenizer(column_list, ",");
		
		File imp_dir = new File(import_dir);
		File[] listFile = imp_dir.listFiles();
			for (File file : listFile) {
				// Lấy dữ liệu các trường của bảng log_file dựa theo file_name.
				Log_file log_file= dp.getControlDb().selectAllFieldLogFile(file.getName(), "log_file");
				// Trạng thái của file hiện tại đã ER || TR || ERRO
				String file_status=log_file.getFile_status();
				// Lấy ra config_id
				int config_id= configuration.getConfig_id();
				// Từ dữ liệu log_file ta lấy ra config_id so sánh với id_config truyền vào nếu bằng nhau
				//và trạng thái là ER (sẵn sàng load vào staging)
				if (log_file.getData_file_config_id()==config_id&& file_status.equals(FILE_STATUS_READY)) {
					String values = "";
					// Kiểm tra nếu file là dạng .txt thì đọc readValuesTXT
					if (file.getName().indexOf(".txt")!=-1 ) {
						values = dp.readValuesTXT(file, delim, strToken.countTokens() );
						// Kiểm tra nếu file là dạng .xlsx thì đọc readValuesXLSX
					} else if (file.getName().indexOf(".xlsx")!=-1) {
						try {
							values = dp.readValuesXLSX(file, strToken.countTokens());
						} catch (org.apache.poi.openxml4j.exceptions.NotOfficeXmlFileException e) {
							System.out.println(e.getMessage());
						}
					}
					// Kiểm tra nếu file là dạng .csv thì đọc readValuesCSV
					else if(file.getName().indexOf(".csv")!=-1){
						System.out.println("CSV");
					}
					// Nếu dữ liệu đọc được khác null và không rỗng
					if (values != null && !values.isEmpty()) {
						String timestamp= getCurrentTime();
						String staging_load_count= dp.getRows()+ "";
						// Tiến hành ghi dữ
						controlDb.truncateTable(controlDb.getTarget_db_name(), target_table);
						isLoadedToStaging= dp.writeDataToStagingDB(column_list, target_table, values);
						// Hậu xử lý sau khi đã load vào staging ( di chuyển file, ghi log)
						post_processing(success_dir, error_dir,  file, timestamp, staging_load_count); 
					}
					 
				}
			}
		return false;
	}
	public void post_processing(String success_dir, String error_dir, File file, String timestamp, String staging_load_count) throws NumberFormatException, ParseException, SQLException{
		if(isLoadedToStaging){
			if (moveFile(success_dir, file)){
				// Lấy id_file dựa vào fileName cung cấp cho bước 3, để load vào warehouse dựa vào id.
				System.out.println(file.getName()+ ": TR");
				System.out.println(timestamp);
				// Cập nhật log_file sau khi đưa vào staging khi thành công trạng thái là TR: sẵn sàng cho transform bước 3.
				dp.getControlDb().updateLogAfterLoadingIntoStagingDB(TABLE_LOG_FILE, FILE_STATUS_TRANSFORM, timestamp, staging_load_count, file.getName());
				int id_file=dp.getControlDb().selectIDFile("data_file_id", TABLE_LOG_FILE, file.getName());
				dw.loadToDW(id_file);
			  }
		}
		 else{
			System.out.println(file.getName()+ ": ERRO");
			System.out.println(timestamp);
			if (moveFile(error_dir, file))
			// Cập nhật log_file sau khi đưa vào staging khi thất bại trạng thái là ERROR.
		    dp.getControlDb().updateLogAfterLoadingIntoStagingDB(TABLE_LOG_FILE, FILE_STATUS_ERRO, timestamp, staging_load_count, file.getName());
		}
		
	}
	// Phương thức thực hiện di chuyển file vào thư mục khác, sau khi ghi vào staging.
	// target_dir là thư mục muốn di chuyển: error_dir (khi ghi vào staging thành công), success_dir ( khi ghi vào staging thất bại).
	private boolean moveFile(String target_dir, File file) {
		try {
			BufferedInputStream bReader = new BufferedInputStream(new FileInputStream(file));
			BufferedOutputStream bWriter = new BufferedOutputStream(
					new FileOutputStream(target_dir + File.separator + file.getName()));
			byte[] buff = new byte[1024 * 10];
			int data = 0;
			while ((data = bReader.read(buff)) != -1) {
				bWriter.write(buff, 0, data);
			}
			bReader.close();
			bWriter.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			file.delete();
		}
	}

	
	public void id_SCP_download(int id_scp) throws SQLException{
	//	pscp.downloadFilePSCP(id_scp);
		PSCPProcess pscp=new PSCPProcess();
		SCP_DownLoad scp_download=  pscp.selectAllField(id_scp, "scp_download");
		loadFileStatus(dp, "log_file", scp_download.getConfig_id());
	}
	public void userInputToDownload() throws NumberFormatException, SQLException, ParseException{
		while(true){
			
			String user="Nhap ma scp muon download.\n"
					+ "1. sinhvien \n"
					+ "2. monhoc\n"
					+ "3. lophoc\n"
					+ "4. dangky\n"
					+ "5. load to staging";
			System.out.println(user);
			int id_scp= Integer.parseInt(sc.nextLine());
			switch (id_scp) {
			case 1:
				System.out.println("Ban da chon tai id_scp:\t" +id_scp);
			    id_SCP_download(id_scp);
				break;
			case 2:
				System.out.println("Ban da chon tai id_scp:\t" +id_scp);
			    id_SCP_download(id_scp);
				break;
			case 3:
				System.out.println("Ban da chon tai id_scp:\t" +id_scp);
			    id_SCP_download(id_scp);
				break;
			case 4:
				System.out.println("Ban da chon tai id_scp:\t" +id_scp);
			    id_SCP_download(id_scp);
				break;
            case 5:
            	userInputToExtractStaging();
				
				break;

			default:
				break;
			}
			
		}
	}
	public void userInputToExtractStaging() throws NumberFormatException, SQLException, ParseException{
		while(true){
			String user="Nhap ma can dua vao staging.\n"
					+ "1. sinhvien \n"
					+ "2. monhoc\n"
					+ "3. lophoc\n"
					+ "4. dangky\n"
					+ "d. download";
			System.out.println(user);
			int id_config= 0;
			try {
				id_config= Integer.parseInt(sc.nextLine());
			} catch (Exception e) {
				userInputToDownload();
			}
			extractToStagingDB(dp, id_config );
			
		}
	}
	
	public static void main(String[] args) throws SQLException, NumberFormatException, ParseException {
		Staging staging=new Staging();
		staging.userInputToDownload();
       //loadToDW(int id_file);
	}
}

//sinhvien.chieu.nhom2.xlsx
//sinhvien_sang.nhom9.xlsx
//sinhvien_sang.nhom11.xlsx