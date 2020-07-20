package etl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import control.Configuration;
import download.DownloadPSCP;
import mail.MailConfig;
import mail.SendMail;
import utils.ControlDB;
import warehouse.DataWarehouse;

public class Staging {
	private DownloadPSCP pscp=null;
	private ArrayList<String> configNames=null;
	private DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
	private LocalDateTime now = LocalDateTime.now();
	private String timestamp = dtf.format(now); 
	private DataWarehouse dw=new DataWarehouse();
	
	private static final String FILE_STATUS_READY="ER";
	private static final String FILE_STATUS_TRANSFORM="TR";
	private static  final String FILE_STATUS_ERRO="ERRO";
	private static final String FILE_STATUS_SUCCESS="SUCCESS";
	
	public Staging() throws SQLException{
		this.configNames=new ArrayList<>();
		this.pscp=new DownloadPSCP();
		this.pscp.downloadFilePSCP();
		
	}

	public void addConfigName(String configName ){
		this.configNames.add(configName);
	}
	
	public void loadFileStatus(DataProcess dp, String table_name){
		for (int i = 0; i < this.configNames.size(); i++) {
			String import_dir = dp.getControlDb().selectField("import_dir", this.configNames.get(i));
			String config_id = dp.getControlDb().selectField("config_id", this.configNames.get(i));
		
			File imp_dir = new File(import_dir);
			if (imp_dir.exists()) {
				File[] listFile = imp_dir.listFiles();
				if(listFile.length>0){
				for(File f: listFile){
						dp.getControlDb().insertLogFileStatus(table_name, f.getName(), config_id, FILE_STATUS_READY,timestamp);
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
		
	}
	
	public boolean extractToStagingDB(DataProcess dp) throws SQLException{
		for (String config_name : this.configNames) {
			
		Configuration configuration = dp.getControlDb().selectAllFieldConfiguration(config_name);
			
		//int config_id= configuration.getConfig_id();
		String target_table = configuration.getTarget_table();
		String file_type = configuration.getFile_type();
		String import_dir = configuration.getImport_dir();
		String success_dir= configuration.getSuccess_dir();
		String error_dir = new Configuration().getError_dir();
		String delim = configuration.getDelimmiter();
		String column_list =  configuration.getColumn_list();
		String column_list_format= configuration.getColumn_list_format();
		
		
//		if (!dp.getControlDb().tableExists(target_table)) {
//			System.out.println(column_list_format);
//			dp.getControlDb().createTable(target_table, column_list_format, column_list);
//		}
		
		System.out.println(import_dir);
		StringTokenizer strToken= new StringTokenizer(column_list,",");
		
		File imp_dir = new File(import_dir);
			String extention = "";
			File[] listFile = imp_dir.listFiles();
			for (File file : listFile) {
				String file_status=dp.getControlDb().selectFileStatus("file_status", "log_file",file.getName());
				if (file.getName().indexOf(file_type) != -1 && file_status.equals(FILE_STATUS_READY)) {
					String values = "";
					System.out.println(file_type);
					if (file_type.equals(".txt") ) {
						values = dp.readValuesTXT(file, delim, 11);
						extention = ".txt";
					} else if (file_type.equals(".xlsx")) {
						values = dp.readValuesXLSX(file, strToken.countTokens());
						extention = ".xlsx";
					}
					
					 
					
					if (values != null) {
						String table = "log_file";
						// count line
						String stagin_load_count = "";
						try {
							stagin_load_count = countLines(file, extention) + "";
						} catch (InvalidFormatException
								| org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
							e.printStackTrace();
						}
						
						if (dp.writeDataToStagingDB(column_list, target_table, values)) {
							System.out.println(file.getName()+"TR");
							file_status = FILE_STATUS_TRANSFORM; 
							
							if (moveFile(success_dir, file)){
							int id_file=dp.getControlDb().selectIDFile("data_file_id", "log_file", file.getName());
							//dw.loadToDW(id_file);
							System.out.println(timestamp);
							dp.getControlDb().updateLogAfterLoadingIntoStagingDB(table, file_status,  timestamp, stagin_load_count, file.getName());
							}
						;

						} else {
							file_status = FILE_STATUS_ERRO;
							System.out.println(file.getName()+ "ERRO");
							System.out.println(timestamp);
							if (moveFile(error_dir, file))
						    dp.getControlDb().updateLogAfterLoadingIntoStagingDB(table, file_status, timestamp, stagin_load_count, file.getName());
								;

						}
					}
					 
				}
			}
		}
		return false;
	}
	
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

	private int countLines(File file, String extention)
			throws InvalidFormatException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		int result = 0;
		XSSFWorkbook workBooks = null;
		try {
			if (extention.indexOf(".txt") != -1) {
				BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				String line;
				while ((line = bReader.readLine()) != null) {
					if (!line.trim().isEmpty()) {
						result++;
					}
				}
				bReader.close();
			} else if (extention.indexOf(".xlsx") != -1) {
				workBooks = new XSSFWorkbook(file);
				XSSFSheet sheet = workBooks.getSheetAt(0);
				Iterator<Row> rows = sheet.iterator();
				rows.next();
				while (rows.hasNext()) {
					rows.next();
					result++;
				}
				return result;
			}

		} catch (IOException | org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
			e.printStackTrace();
		} finally {
			if (workBooks != null) {
				try {
					workBooks.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}
	
	public static void main(String[] args) throws SQLException {
		Staging staging=new Staging();
		//staging.addConfigName("file_student_xlsx");
		//staging.addConfigName("file_txt");
		staging.addConfigName("file_monhoc_xlsx");
		ControlDB controlDb=new ControlDB("controldb","stagingdb", "configuration");
		// 
		DataProcess dp=new DataProcess();
		dp.setControlDb(controlDb);
		System.out.println(staging.configNames.size());
	 // staging.loadFileStatus(dp,"log_file");
      staging.extractToStagingDB(dp);
       //loadToDW(int id_file);
	}
}
//sinhvien.chieu.nhom2 kieu format date