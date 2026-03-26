
/*****************************************************************************
This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
 ******************************************************************************/

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * Custom JUL formatter for subscriber log output.
 *
 * Text format (default):
 *   2026-03-26 10:23:01.123 [upload] INFO SubmissionProcessor [inst:uuid-123] message
 *
 * JSON format (smap.log.format=json in smap-logging.properties):
 *   {"ts":"...","mode":"upload","level":"INFO","logger":"...","inst":"...","msg":"..."}
 *
 * The instanceId is read from SubmissionContext (thread-local).
 * The subscriber mode (upload/forward) is read from the system property smap.subscriber.mode.
 */
public class SmapLogFormatter extends Formatter {

	private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter
			.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
			.withZone(ZoneOffset.UTC);

	private final boolean json;

	public SmapLogFormatter(boolean json) {
		this.json = json;
	}

	@Override
	public String format(LogRecord record) {
		String mode = System.getProperty("smap.subscriber.mode", "?");
		String instanceId = SubmissionContext.get();
		String timestamp = TS_FORMAT.format(Instant.ofEpochMilli(record.getMillis()));
		String level = record.getLevel().getName();
		String logger = shortName(record.getLoggerName());
		String message = formatMessage(record);

		if (json) {
			return formatJson(timestamp, mode, level, logger, instanceId, message, record);
		} else {
			return formatText(timestamp, mode, level, logger, instanceId, message, record);
		}
	}

	private String formatText(String ts, String mode, String level, String logger,
			String instanceId, String message, LogRecord record) {
		StringBuilder sb = new StringBuilder(128);
		sb.append(ts)
		  .append(" [").append(mode).append("]")
		  .append(" ").append(level)
		  .append(" ").append(logger);

		if (!"-".equals(instanceId)) {
			sb.append(" [inst:").append(instanceId).append("]");
		}

		sb.append(" ").append(message).append("\n");

		if (record.getThrown() != null) {
			StringWriter sw = new StringWriter();
			record.getThrown().printStackTrace(new PrintWriter(sw));
			sb.append(sw);
		}
		return sb.toString();
	}

	private String formatJson(String ts, String mode, String level, String logger,
			String instanceId, String message, LogRecord record) {
		StringBuilder sb = new StringBuilder(256);
		sb.append("{")
		  .append("\"ts\":\"").append(ts).append("\"")
		  .append(",\"mode\":\"").append(escape(mode)).append("\"")
		  .append(",\"level\":\"").append(level).append("\"")
		  .append(",\"logger\":\"").append(escape(logger)).append("\"");

		if (!"-".equals(instanceId)) {
			sb.append(",\"inst\":\"").append(escape(instanceId)).append("\"");
		}

		sb.append(",\"msg\":\"").append(escape(message)).append("\"");

		if (record.getThrown() != null) {
			StringWriter sw = new StringWriter();
			record.getThrown().printStackTrace(new PrintWriter(sw));
			sb.append(",\"stack\":\"").append(escape(sw.toString())).append("\"");
		}

		sb.append("}\n");
		return sb.toString();
	}

	private String shortName(String name) {
		if (name == null) return "?";
		int dot = name.lastIndexOf('.');
		return (dot >= 0) ? name.substring(dot + 1) : name;
	}

	private String escape(String s) {
		if (s == null) return "";
		return s.replace("\\", "\\\\")
		        .replace("\"", "\\\"")
		        .replace("\n", "\\n")
		        .replace("\r", "\\r")
		        .replace("\t", "\\t");
	}
}
