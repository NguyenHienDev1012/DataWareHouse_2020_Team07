package org.ecepvn.date_dim;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import javax.swing.JSpinner.DateEditor;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.Period;

/*
 * Code : son.tran@ecepvn.org
 * Version : 2015/11/21 v01
 */
public class Date_Dim {
	public static final String OUT_FILE = "date_dim_without_quarter.csv";
	public static final int NUMBER_OF_RECORD = 7670;
	public static final String TIME_ZONE = "PST8PDT";

	public static void main(String[] args) {
		DateTimeZone dateTimeZone = DateTimeZone.forID(TIME_ZONE);
		int count = 0;
		int date_sk = 0;
		int month_since_2005 = 1;
		int day_since_2005 = 0;
		int quarter_since_2005_temp = 0;
		int quarter_temp = 1;
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
		DateTime startDateTimeforMonth = startDateTime.plus(Period.days(1));
		while (count <= NUMBER_OF_RECORD) {
			startDateTime = startDateTime.plus(Period.days(1));
			Date startDate = startDateTime.toDate();
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(startDate);
			Date startTime = calendar.getTime();
			// Date_SK
			date_sk += 1; // 1
			SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
			// Full Date
			String full_date = dt.format(calendar.getTime()); // 2
			// Day Since 2005
			day_since_2005 += 1; // 3
			int month_since_2005_temp = Months.monthsBetween(
					startDateTimeforMonth.toLocalDate(),
					startDateTime.toLocalDate()).getMonths();
			// Month Since 2005
			month_since_2005 = month_since_2005_temp + 1;
			// Day of Week
			String day_of_week = calendar.getDisplayName(Calendar.DAY_OF_WEEK,
					Calendar.LONG, Locale.US); // 5
			// Calendar Month
			String calendar_month = calendar.getDisplayName(Calendar.MONTH,
					Calendar.LONG, Locale.US); // 6
			dt = new SimpleDateFormat("yyyy");
			// Calendar Year
			String calendar_year = dt.format(calendar.getTime()); // 7
			String calendar_month_short = calendar.getDisplayName(
					Calendar.MONTH, Calendar.SHORT, Locale.US);
			// Calendar Year Month
			String calendar_year_month = calendar_year + "-"
					+ calendar_month_short; // 8
			// Date of Month
			int day_of_month = calendar.get(Calendar.DAY_OF_MONTH); // 9
			// Day of Year
			int day_of_year = calendar.get(Calendar.DAY_OF_YEAR); // 10
			Calendar calendar_temp = calendar;
			// Week of Year Sunday
			int week_of_year_sunday = lastDayOfLastWeek(calendar_temp).get(
					Calendar.WEEK_OF_YEAR); // 11
			int year_sunday = lastDayOfLastWeek(calendar).get(Calendar.YEAR);
			// Year Week Sunday
			String year_week_sunday = ""; // 12
			if (week_of_year_sunday < 10) {
				year_week_sunday = year_sunday + "-" + "W0"
						+ week_of_year_sunday;
			} else {
				year_week_sunday = year_sunday + "-" + "W"
						+ week_of_year_sunday;
			}
			calendar_temp = Calendar.getInstance(Locale.US);
			calendar_temp.setTime(calendar.getTime());
			calendar_temp.set(Calendar.DAY_OF_WEEK,
					calendar_temp.getFirstDayOfWeek());
			dt = new SimpleDateFormat("yyyy-MM-dd");
			// Week Sunday Start
			String week_sunday_start = dt.format(calendar_temp.getTime()); // 13
			DateTime startOfWeek = startDateTime.weekOfWeekyear()
					.roundFloorCopy();
			// Week of Year Monday
			int week_of_year_monday = startOfWeek.getWeekOfWeekyear(); // 14
			dt = new SimpleDateFormat("yyyy");
			int year_week_monday_temp = startOfWeek.getYear();
			// Year Week Monday
			String year_week_monday = "";
			if (week_of_year_monday < 10) {
				year_week_monday = year_week_monday_temp + "-W0"
						+ week_of_year_monday;
			} else {
				year_week_monday = year_week_monday_temp + "-W"
						+ week_of_year_monday;
			}
			dt = new SimpleDateFormat("yyyy-MM-dd");
			// Week Monday Start
			String week_monday_start = dt.format(startOfWeek.toDate()); // 16
			// Quarter Since 2005
			int month = startDateTime.getMonthOfYear();
			int quarter = month % 3 == 0 ? (month / 3) : (month / 3) + 1;
			if (quarter == quarter_temp) {
				quarter_since_2005_temp = quarter_since_2005_temp + 1;
				quarter_temp += 1;
				if (quarter_temp > 4) {
					quarter_temp = 1;
				}
			}
			
			int quarter_since_2005 = 0;
			quarter_since_2005 += quarter_since_2005_temp;
			
			// Quarter of Year
			String quarter_year = startDateTime.getYear() + "";
			String quarter_of_year_temp = getQuarter(startDateTime.getMonthOfYear());
			String quarter_of_year = quarter_year + "-" + quarter_of_year_temp;
			// Holiday
			String holiday = "Non-Holiday"; // 17
			// Day Type
			String day_type = isWeekend(day_of_week); // 18
			String output = date_sk + "," + full_date + "," + day_since_2005
					+ "," + month_since_2005 + "," + day_of_week + ","
					+ calendar_month + "," + calendar_year + ","
					+ calendar_year_month + "," + day_of_month + ","
					+ day_of_year + "," + week_of_year_sunday + ","
					+ year_week_sunday + "," + week_sunday_start + ","
					+ week_of_year_monday + "," + year_week_monday + ","
					+ week_monday_start + "," + holiday + "," + day_type;
			// System.out.println(output);
			count++;
			// Printout Data to File
			pr.println(output);
			pr.flush();
		}
	}

	public static String getWeekOfYearSunday(Calendar calendar) {
		Date date = getFirstDayOfWeekDate(calendar);
		Calendar newCalendar = Calendar.getInstance(Locale.US);
		newCalendar.setTime(date);
		int result = newCalendar.getWeeksInWeekYear();
		return "" + result;
	}

	public static String getFirstDayOfWeekString(Calendar calendar) {
		int week = calendar.get(Calendar.DAY_OF_WEEK);
		SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd");
		Date now = calendar.getTime();
		Date temp = new Date(now.getTime() - 24 * 60 * 60 * 1000 * (week - 1));
		String result = dt.format(temp);
		return result;
	}

	public static Date getFirstDayOfWeekDate(Calendar calendar) {
		int week = calendar.get(Calendar.DAY_OF_WEEK);
		Date now = calendar.getTime();
		Date temp = new Date(now.getTime() - 24 * 60 * 60 * 1000 * (week - 1));
		return temp;
	}

	public static Calendar getDateOfMondayInCurrentWeek(Calendar c) {
		c.setFirstDayOfWeek(Calendar.MONDAY);
		int today = c.get(Calendar.DAY_OF_WEEK);
		c.add(Calendar.DAY_OF_WEEK, -today + Calendar.MONDAY);
		return c;
	}

	public static Calendar firstDayOfLastWeek(Calendar c) {
		c = (Calendar) c.clone();
		// last week
		c.add(Calendar.WEEK_OF_YEAR, -1);
		// first day
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
		return c;
	}

	public static Calendar lastDayOfLastWeek(Calendar c) {
		c = (Calendar) c.clone();
		// first day of this week
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
		// last day of previous week
		c.add(Calendar.DAY_OF_MONTH, -1);
		return c;
	}

	/*
	 * Check if Given day is weekend (Saturday or Sunday)
	 */
	public static String isWeekend(String day) {
		if (day.equalsIgnoreCase("Saturday") || day.equalsIgnoreCase("Sunday")) {
			return "Weekend";
		} else {
			return "Weekday";
		}
	}

	/**
	 * 
	 */
	public static String getQuarter(int month) {
		int quarter = month % 3 == 0 ? (month / 3) : (month / 3) + 1;
		String result = "Q" + quarter;
		return result;
	}
}
