package org.ecepvn.month_dim;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.Period;

public class Month_Dim {
	public static final String OUT_FILE = "month_dim.csv";
	public static final int NUMBER_OF_RECORD = 251;
	public static final String TIME_ZONE = "PST8PDT";

	public static void main(String[] args) {
		DateTimeZone dateTimeZone = DateTimeZone.forID(TIME_ZONE);
		int count = 0;
		int month_sk = 0;
		String calendar_year_month = "";
		int month_since_2005 = 0;
		int date_sk_start = 0;
		int date_sk_end = 0;
		PrintWriter pr = null;
		try {
			File file = new File(OUT_FILE);
			if (file.exists()) {
				file.delete();
			}
			pr = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
		} catch (Exception e) {
			e.printStackTrace();
		}
		DateTime startDateTime = new DateTime(2004, 12, 31, 0, 0, 0);
		while (count <= NUMBER_OF_RECORD) {
			startDateTime = startDateTime.plus(Period.months(1));
			Date startDate = startDateTime.toDate();
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(startDate);
			int calendar_year = startDateTime.getYear();
			String calendar_short_month = calendar.getDisplayName(
					Calendar.MONTH, Calendar.SHORT, Locale.US);
			calendar_year_month = calendar_year + "-" + calendar_short_month;
			date_sk_start = date_sk_end + 1; 
			date_sk_end = date_sk_start + calendar.getActualMaximum(Calendar.DAY_OF_MONTH) - 1;
			month_sk++;
			month_since_2005++;
			count++;
			String result = month_sk + "," + calendar_year_month + "," + month_since_2005 + "," + date_sk_start + "," + date_sk_end;
			pr.println(result);
			pr.flush();
		}
	}
}
