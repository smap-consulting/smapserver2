
/*****************************************************************************
This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
 ******************************************************************************/

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hot-reload log configuration for the subscriber process.
 *
 * Reads {basePath}/settings/smap-logging.properties (or the path given by
 * the system property smap.log.config) every 30 seconds and applies any
 * changes to the root logger level and output format without restarting.
 *
 * Supported properties:
 *   smap.log.level  = SEVERE | WARNING | INFO | FINE  (default: INFO)
 *   smap.log.format = text | json                     (default: text)
 */
public class LogConfig {

	private static final Logger log = Logger.getLogger(LogConfig.class.getName());

	private static volatile Level currentLevel = Level.INFO;
	private static volatile String currentFormat = "text";
	private static String configPath;

	/**
	 * Call once from Manager.main() before starting any processor threads.
	 * Installs SmapLogFormatter, applies initial config, and starts the
	 * background polling thread.
	 */
	public static void init(String basePath) {
		configPath = System.getProperty("smap.log.config",
				basePath + "/settings/smap-logging.properties");

		// Replace default JUL handler with our formatter
		Logger rootLogger = Logger.getLogger("");
		for (Handler h : rootLogger.getHandlers()) {
			rootLogger.removeHandler(h);
		}
		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new SmapLogFormatter(false));
		handler.setLevel(Level.INFO);
		rootLogger.addHandler(handler);
		rootLogger.setLevel(Level.INFO);

		// Apply config file if it exists
		applyConfig();

		// Daemon thread polls config file every 30 seconds
		Thread poller = new Thread(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					Thread.sleep(30_000);
					applyConfig();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}
		});
		poller.setDaemon(true);
		poller.setName("smap-log-config-poller");
		poller.start();
	}

	private static void applyConfig() {
		File f = new File(configPath);
		if (!f.exists() || !f.canRead()) return;

		Properties props = new Properties();
		try (FileInputStream fis = new FileInputStream(f)) {
			props.load(fis);
		} catch (Exception e) {
			System.err.println("smap-logging: could not read " + configPath + ": " + e.getMessage());
			return;
		}

		// Update level
		String levelStr = props.getProperty("smap.log.level", "INFO").trim().toUpperCase();
		Level newLevel = parseLevel(levelStr);
		if (newLevel != null && !newLevel.equals(currentLevel)) {
			currentLevel = newLevel;
			setRootLevel(newLevel);
			log.info("Log level changed to " + newLevel.getName());
		}

		// Update format
		String format = props.getProperty("smap.log.format", "text").trim().toLowerCase();
		if (!format.equals(currentFormat)) {
			currentFormat = format;
			applyFormatter("json".equals(format));
			log.info("Log format changed to " + format);
		}
	}

	private static Level parseLevel(String s) {
		switch (s) {
			case "SEVERE":  case "ERROR":   return Level.SEVERE;
			case "WARNING": case "WARN":    return Level.WARNING;
			case "INFO":                    return Level.INFO;
			case "FINE":    case "DEBUG":   return Level.FINE;
			case "ALL":                     return Level.ALL;
			default:
				System.err.println("smap-logging: unknown level '" + s + "', keeping current");
				return null;
		}
	}

	private static void setRootLevel(Level level) {
		Logger rootLogger = Logger.getLogger("");
		rootLogger.setLevel(level);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(level);
		}
	}

	private static void applyFormatter(boolean json) {
		SmapLogFormatter formatter = new SmapLogFormatter(json);
		Logger rootLogger = Logger.getLogger("");
		for (Handler h : rootLogger.getHandlers()) {
			h.setFormatter(formatter);
		}
	}
}
