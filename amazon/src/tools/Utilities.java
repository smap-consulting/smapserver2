package tools;

import java.util.logging.Logger;

import org.smap.notifications.interfaces.AudioProcessing;

public class Utilities {
	
	private static Logger log = Logger.getLogger(AudioProcessing.class.getName());
	
	/*
	 * Create media to another format
	 */
	public static boolean convertMedia(String in, String out) {

		boolean status = false;
		
		String cmd = "/smap_bin/convertMedia.sh " + in + " " + out;
		log.info("Exec: " + cmd);
		try {

			Process proc = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", cmd });
			int code = proc.waitFor();
			log.info("Convrt media processing finished with status:" + code);
			if(code > 0) {
				int len;
				if ((len = proc.getErrorStream().available()) > 0) {
					byte[] buf = new byte[len];
					proc.getErrorStream().read(buf);
					log.info("Command error:\t\"" + new String(buf) + "\"");
				}
			} else {
				status = true;
				int len;
				if ((len = proc.getInputStream().available()) > 0) {
					byte[] buf = new byte[len];
					proc.getInputStream().read(buf);
					log.info("Completed convert media process:\t\"" + new String(buf) + "\"");
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return status;
	}

}
