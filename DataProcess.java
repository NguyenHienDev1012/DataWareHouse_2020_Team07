
package etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.zip.ZipException;

import javax.security.auth.Subject;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import utils.ControlDB;


public class DataProcess {
	public static final String NUMBER_REGEX = "^[0-9]+$";
	private ControlDB controlDb;
	public static final String DATE_FORMAT = "dd/MM/yyyy";
	public static int rows= 0;
	
	public String readLines(String value, String delim) {
		String values = "";
		StringTokenizer stoken = new StringTokenizer(value, delim);
		int countToken = stoken.countTokens();
		String lines = "(";
		for (int j = 0; j < countToken; j++) {
			String token = stoken.nextToken();
			if (Pattern.matches(NUMBER_REGEX, token)) {
				lines += (j == countToken - 1) ? token.trim() + ")," : token.trim() + ",";
			} else {
				lines += (j == countToken - 1) ? "'" + token.trim() + "')," : "'" + token.trim() + "',";
			}
			values += lines;
			lines = "";
		}
		System.out.println(values);
		return values;
	}


	public String readValuesTXT(File s_file, String delim, int field_quantity) {
		String values = "";
		int countRows=0;
		try {
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file)));
			String line;
			line=bReader.readLine();
			while ((line = bReader.readLine()) != null) {
				countRows++;
				System.out.println(line);
				values += readLines(line, delim);
			}
			bReader.close();
			setRows(countRows);
			countRows= 0;
			return values.substring(0, values.length() - 1);

		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	
	public String readValuesXLSX(File s_file, int field_quantity)   {
		String values = "";
		String value = "";
		String delim = "|";
		int countRows= 0;
		try {
			FileInputStream fileIn = new FileInputStream(s_file);
			XSSFWorkbook workBook = new XSSFWorkbook(fileIn);
			XSSFSheet sheet = workBook.getSheetAt(0);
			Iterator<Row> rows = sheet.iterator();
			
			if (rows.next().cellIterator().next().getCellType().equals(CellType.NUMERIC)) {
				rows = sheet.iterator();
			}
			while (rows.hasNext()) {
				countRows++;
				Row row = rows.next();
				
				for (int i = 0; i <field_quantity; i++) {
					Cell cell= row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
					CellType cellType = cell.getCellType();
					switch (cellType) {
					case NUMERIC:
						if (DateUtil.isCellDateFormatted(cell)) {
							SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
							value += dateFormat.format(cell.getDateCellValue()) + delim;
						} else {
							value += (long) cell.getNumericCellValue() + delim;
						}
						break;
					case STRING:
						value += cell.getStringCellValue() + delim;
						break;
					case FORMULA:
						switch (cell.getCachedFormulaResultType()) {
						case NUMERIC:
							value += (long) cell.getNumericCellValue() + delim;
							break;
						case STRING:
							value += cell.getStringCellValue() + delim;
							break;
						default:
							value += " " + delim;
							break;
						}
						break;
					default:
						value += " " + delim;
						break;
					}
				}
				values += readLines(value, delim);
				value = "";
			}
			setRows(countRows);
			countRows= 0;
			workBook.close();
			fileIn.close();
			return values.substring(0, values.length() - 1);
		} catch (Exception e) {
			return null;
		}
	}

	//Ghi dữ liệu vào staging db
	public boolean writeDataToStagingDB(String column_list, String target_table, String values) {
		if (controlDb.insertValues(column_list, values, target_table))
			return true;
		return false;
	}
	public int getRows(){
		return rows;
	}
	public int setRows(int inputrRows){
		return rows= inputrRows;
	}

	public ControlDB getControlDb() {
		return controlDb;
	}

	public void setControlDb(ControlDB controlDb) {
		this.controlDb = controlDb;
	}
   

}
