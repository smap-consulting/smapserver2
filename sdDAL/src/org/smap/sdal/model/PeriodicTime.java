package org.smap.sdal.model;

import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;

/*
 * Attempt to convert times used in periodic time to UTC from local and back again
 * This is imperfect as full dates are not used so changes in local time due to daylight saving are not well handled
 */
public class PeriodicTime {
	private ZonedDateTime lZdt = null;			// Local date time with time zone
	private ZonedDateTime utcZdt = null;		// UTC date time with time zone
	private String tz;							// The local time zone
	
	public PeriodicTime(String tz) {
		this.tz = tz;
	}
	
	public String toString() {
		return "Local: " + lZdt +  " UTC: " + utcZdt;
	}
	
	public void setLocalTime(String time) throws Exception {

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
		ZoneId localZoneId = TimeZone.getTimeZone(tz).toZoneId();
		lZdt = ZonedDateTime.of(localDate, localTime, localZoneId);

		// Convert to utc zoned date time
		ZoneId utcZoneId = TimeZone.getTimeZone("UTC").toZoneId();
		utcZdt = lZdt.withZoneSameInstant(utcZoneId);
	}
	
	public void setUtcTime(Time sqlTime) {
		LocalDate utcDate = LocalDate.now();
		LocalTime utcTime = sqlTime.toLocalTime();
		utcZdt = ZonedDateTime.of(utcDate, utcTime, TimeZone.getTimeZone("UTC").toZoneId());
		
		lZdt = utcZdt.withZoneSameInstant(TimeZone.getTimeZone(tz).toZoneId());
	}
	
	public Time getUtcTime() {		
		return Time.valueOf(utcZdt.toLocalTime());
	}
	
	public String getLocalTime() {		
		return String.format("%02d", lZdt.getHour()) + ":" + String.format("%02d", lZdt.getMinute());
	}

}
