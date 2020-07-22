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
import mail.MailConfig;
import mail.SendMail;
import utils.ControlDB;
import warehouse.DataWarehouse;

public class Staging {
	private DownloadPSCP pscp=null;
	private DataWarehouse dw=new DataWarehouse();
	private Scanner sc = new Scanner(System.in);
	private ControlDB controlDb=new ControlDB("controldb","stagingdb", "configuration");
	private DataProcess dp=new DataProcess();
	
	private static final String FILE_STATUS_READY="ER";
	private static final String FILE_STATUS_TRANSFORM="TR";
	private static  final String FILE_STATUS_ERRO="ERRO";
	private static final String FILE_STATUS_SUCCESS="SUCCESS";
	
	public Staging() throws SQLException{
		dp.setControlDb(controlDb);
		this.pscp=new DownloadPSCP();
		
	}

	
	public void loadFileStatus(DataProcess dp, String table_name, int id_config) throws SQLException{
		String timestamp= getCurrentTime();
		 
			Configuration configuration=dp.getControlDb().selectAllFieldConfigurationByConfigId((id_config));
			String import_dir = configuration.getImport_dir();
			int config_id = configuration.getConfig_id();
			
			ArrayList<String> listFileNameCurrent=dp.getControlDb().selectAllFileNameInLogFile("log_file");

			File imp_dir = new File(import_dir);
			if (imp_dir.exists()) {
				File[] listFile = imp_dir.listFiles();
				if(listFile.length>0){
				for(File f: listFile){
					if(listFileNameCurrent.size()>0){
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
		
	
	public boolean extractToStagingDB(DataProcess dp, int id_config) throws SQLException{
			
		Configuration configuration = dp.getControlDb().selectAllFieldConfigurationByConfigId(id_config);
		System.out.println(configuration.toString());
			
		String target_table = configuration.getTarget_table();
		String import_dir = configuration.getImport_dir();
		String success_dir= configuration.getSuccess_dir();
		String error_dir = configuration.getError_dir();
		String delim = configuration.getDelimmiter();
		String column_list =  configuration.getColumn_list();
		String column_list_format= configuration.getColumn_list_format();
		
//		if (!dp.getControlDb().tableExists(target_table)) {
//			System.out.println(column_list_format);
//			dp.getControlDb().createTable(target_table, column_list_format, column_list);
//		}
		
		//System.out.println(import_dir);
		StringTokenizer strToken= new StringTokenizer(column_list,",");
		
		File imp_dir = new File(import_dir);
			String extention = "";
			File[] listFile = imp_dir.listFiles();
			for (File file : listFile) {
				
				Log_file log_file= dp.getControlDb().selectAllFieldLogFile(file.getName(), "log_file");
				
				String file_status=log_file.getFile_status();
				int config_id= configuration.getConfig_id();
				
				if (log_file.getData_file_config_id()==config_id&& file_status.equals(FILE_STATUS_READY)) {
					String values = "";
					if (file.getName().indexOf(".txt")!=-1 ) {
						values = dp.readValuesTXT(file, delim, strToken.countTokens() );
						extention = ".txt";
					} else if (file.getName().indexOf(".xlsx")!=-1) {
						try {
							values = dp.readValuesXLSX(file, strToken.countTokens());
						} catch (java.util.zip.ZipException e) {
							System.out.println(e.getMessage());
						}
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
						String timestamp= getCurrentTime();
						 
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
		return false;
	}
	public String getCurrentTime() {
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		return dtf.format(now);
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
	
	public void id_SCP_download(int id_scp) throws SQLException{
		pscp.downloadFilePSCP(id_scp);
		PSCPProcess pscp=new PSCPProcess();
		SCP_DownLoad scp_download=  pscp.selectAllField(id_scp, "scp_download");
		loadFileStatus(dp, "log_file", scp_download.getConfig_id());
	}
	public void userInputToDownload() throws NumberFormatException, SQLException{
		while(true){
			
			String user="Nhap ma scp muon download.\n"
					+ "1. student \n"
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
	public void userInputToExtractStaging() throws NumberFormatException, SQLException{
		while(true){
			String user="Nhap ma can extract vao staging.\n"
					+ "1. student \n"
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
	
	public static void main(String[] args) throws SQLException {
		Staging staging=new Staging();
		staging.userInputToDownload();
       //loadToDW(int id_file);
	}
}

//sinhvien.chieu.nhom2.xlsx
//sinhvien_sang.nhom9.xlsx
//sinhvien_sang.nhom11.xlsx
