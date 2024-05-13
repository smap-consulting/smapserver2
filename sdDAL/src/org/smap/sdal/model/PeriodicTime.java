package org.smap.sdal.model;

import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.TimeZone;

/*
 * Attempt to convert times used in periodic time to UTC from local and back again
 * This is imperfect as full dates are not used so changes in local time due to daylight saving are not well handled
 */
public class PeriodicTime {
	public static String DAILY = "daily";
	public static String WEEKLY = "weekly";
	public static String MONTHLY = "monthly";
	public static String YEARLY = "yearly";
	
	private ZonedDateTime lZdt = null;			// Local date time with time zone
	private ZonedDateTime utcZdt = null;		// UTC date time with time zone
	
	private String period;
	private String tz;							// The local time zone
	
	private int localWeekday;
	
	private int utcWeekday;
	
	public PeriodicTime(String period, String tz) {
		this.period = period;
		this.tz = tz;
	}
	
	public String toString() {
		return "Local: " + lZdt +  " UTC: " + utcZdt;
	}
	
	public void setLocalTime(String time, int weekday, int monthday, int month) throws Exception {

		if(time != null) {
			String[] tComp = time.split(":");
			int hour = 0;
			int minute = 0;
			if(tComp.length > 1) {
				hour = Integer.valueOf(tComp[0]);
				minute = Integer.valueOf(tComp[1]);
			} else {
				throw new Exception("Invalid time format: " + time);
			}
	
			// Get local zoned date time
			LocalDate localDate = LocalDate.now();
			LocalTime localTime = LocalTime.of(hour, minute);
			lZdt = ZonedDateTime.of(localDate, localTime, TimeZone.getTimeZone(tz).toZoneId());
			
			// weekday
			this.localWeekday = weekday;
			
			// monthday
			if(period.equals(MONTHLY) || period.equals(YEARLY)) {
				lZdt = lZdt.withDayOfMonth(monthday);
			}
			
			// month
			if(period.equals(YEARLY)) {
				lZdt = lZdt.withMonth(month);
			}
			
			// Set utc zoned date time
			utcZdt = lZdt.withZoneSameInstant(TimeZone.getTimeZone("UTC").toZoneId());
		}
	}
	
	public void setUtcTime(Time sqlTime, int weekday) {
		if(sqlTime != null) {
			LocalDate utcDate = LocalDate.now();
			LocalTime utcTime = sqlTime.toLocalTime();
			utcZdt = ZonedDateTime.of(utcDate, utcTime, TimeZone.getTimeZone("UTC").toZoneId());
			
			// weekday
			this.utcWeekday = weekday;
			
			// Set local zoned date time
			lZdt = utcZdt.withZoneSameInstant(TimeZone.getTimeZone(tz).toZoneId());
		}
	}
	
	public Time getUtcTime() {		
		return utcZdt == null ? null : Time.valueOf(utcZdt.toLocalTime());
	}
	
	public String getLocalTime() {		
		return lZdt == null ? null : String.format("%02d", lZdt.getHour()) + ":" + String.format("%02d", lZdt.getMinute());
	}
	
	/*
	 * Get the UTC day of the week
	 */
	public int getUtcWeekday() {
		if(lZdt == null) {
			return 0;
		}
		int utcWeekday = localWeekday - lZdt.getDayOfYear() + utcZdt.getDayOfYear();		
		return adjustWeekday(utcWeekday);
	}
	
	/*
	 * Get the UTC day of the month
	 */
	public int getUtcMonthday() {
		return utcZdt == null ? 0 : utcZdt.getDayOfMonth();
	}
	
	/*
	 * Get the UTC month
	 */
	public int getUtcMonth() {
		return utcZdt == null ? 0 : utcZdt.getMonth().getValue();
	}
	
	/*
	 * Get the local day of the week
	 */
	public int getLocalWeekday() {
		if(lZdt == null) {
			return 0;
		}
		int localWeekday = utcWeekday + lZdt.getDayOfYear() - utcZdt.getDayOfYear();		
		return adjustWeekday(localWeekday);
	}
	
	/*
	 * Adjust the week day to its range 
	 * Day of week is stored as a number from 0 (sunday) to 6 (saturday)
	 */
	private int adjustWeekday(int weekday) {
		weekday = (weekday < 0) ? 6 : weekday;
		weekday = (weekday > 6) ? 0 : weekday;
		return weekday;
	}

}
