package process;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import utils.ControlDB;

public class Staging {
	private DownloadPSCP pscp=null;
	private ArrayList<String> configNames=null;
	private DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
	private LocalDateTime now = LocalDateTime.now();
	private String timestamp = dtf.format(now); 
	
	private final String FILE_STATUS_READY="ER";
	private final String FILE_STATUS_TRANSFORM="TR";
	private final String FILE_STATUS_ERRO="ERRO";
	private final String FILE_STATUS_SUCCESS="SUCCESS";
	
	public Staging(){
		this.configNames=new ArrayList<>();
		this.pscp=new DownloadPSCP();
		//this.pscp.downloadFilePSCP();
		
	}

	public void addConfigName(String configName ){
		this.configNames.add(configName);
	}
	public void startExtractToDB(DataProcess dp){
		for(String configName: this.configNames){
		//	ExtractToDB(dp, configName);
		}
	}
	
	public void loadFileStatus(DataProcess dp, String table_name){
		for (int i = 0; i < this.configNames.size(); i++) {
			String import_dir = dp.getControlDb().selectField("import_dir", this.configNames.get(i));
			String config_id = dp.getControlDb().selectField("config_id", this.configNames.get(i));
		
			File imp_dir = new File(import_dir);
			if (imp_dir.exists()) {
				File[] listFile = imp_dir.listFiles();
				for(File f: listFile){
						dp.getControlDb().insertLogFileStatus(table_name, f.getName(), config_id, FILE_STATUS_READY,timestamp);
				}
				System.out.println("Load file status successfully!");
			}
			else{
				System.out.println("Path not exists!!!");
				return;
			}
			
		}
		
	}
	
	public boolean extractToStagingDB(DataProcess dp){
		for (int i = 0; i < this.configNames.size(); i++) {
		String target_table = dp.getControlDb().selectField("target_table", this.configNames.get(i));

		if (!dp.getControlDb().tableExists(target_table)) {
			String properties = dp.getControlDb().selectField("properties",this.configNames.get(i));
			System.out.println(properties);
			String column_list = dp.getControlDb().selectField("column_list", this.configNames.get(i));
			dp.getControlDb().createTable(target_table, properties, column_list);
		}
		
		String file_type = dp.getControlDb().selectField("file_type", this.configNames.get(i));
		String import_dir = dp.getControlDb().selectField("import_dir", this.configNames.get(i));
		System.out.println(import_dir);
		String delim = dp.getControlDb().selectField("delimmeter", this.configNames.get(i));
		String column_list = dp.getControlDb().selectField("column_list", this.configNames.get(i));
		
		File imp_dir = new File(import_dir);
			String extention = "";
			File[] listFile = imp_dir.listFiles();
			for (File file : listFile) {
				String file_status=dp.getControlDb().selectFileStatus("file_status", "log_file",file.getName());
				if (file.getName().indexOf(file_type) != -1 && file_status.equals(FILE_STATUS_READY)) {
					String values = "";
					System.out.println(file_type);
					if (file_type.equals(".txt") ) {
						values = dp.readValuesTXT(file, delim);
						extention = ".txt";
					} else if (file_type.equals(".xlsx")) {
						values = dp.readValuesXLSX(file);
						extention = ".xlsx";
					}
					
					 
					
					if (values != null) {
						String table = "log_file";
						String config_id = dp.getControlDb().selectField("config_id", this.configNames.get(i));
						// count line
						String stagin_load_count = "";
						try {
							stagin_load_count = countLines(file, extention) + "";
						} catch (InvalidFormatException
								| org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
							e.printStackTrace();
						}
						String target_dir;
						
						if (dp.writeDataToStagingDB(column_list, target_table, values)) {
							System.out.println("TR");
							file_status = FILE_STATUS_TRANSFORM; 
							target_dir = dp.getControlDb().selectField("success_dir", this.configNames.get(i));
							if (moveFile(target_dir, file)){
								System.out.println(timestamp);
							dp.getControlDb().updateLogAfterLoadingIntoStagingDB(table, file_status, config_id, timestamp, stagin_load_count, file.getName());
							}
						;

						} else {
							file_status = FILE_STATUS_ERRO;
							System.out.println("ERRO");
							System.out.println(timestamp);
							target_dir = dp.getControlDb().selectField("error_dir", this.configNames.get(i));
							if (moveFile(target_dir, file))
						    dp.getControlDb().updateLogAfterLoadingIntoStagingDB(table, file_status, config_id, timestamp, stagin_load_count, file.getName());
								;

						}
					}
					 
				}
				else{
					System.out.println("Don't have any file_status is ready to load into staging db! ");
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
	
	public static void main(String[] args) {
		Staging staging=new Staging();
		staging.addConfigName("file_xlsx");
		//dw.addConfigName("file_txt");
		
		ControlDB controlDb=new ControlDB("controldb","stagingdb", "configuration");
		// 
		DataProcess dp=new DataProcess();
		dp.setControlDb(controlDb);
	    //staging.loadFileStatus(dp,"log_file");
       staging.extractToStagingDB(dp);
       //File f=new File("C:/Users/PC/Desktop/LEARNING/Data/File/sinhvien_sang_nhom14.xlsx");
       //System.out.println(dp.readValuesXLSX(f));
       //loadToDW(int id_file);
	}
}
//sinhvien.chieu.nhom2 kieu format date