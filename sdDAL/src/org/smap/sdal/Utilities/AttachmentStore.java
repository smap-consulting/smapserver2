package org.smap.sdal.Utilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.smap.notifications.interfaces.S3AttachmentUpload;

/*****************************************************************************
 *
 * This file is part of SMAP.
 *
 * SMAP is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * SMAP is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * SMAP. If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

/*
 * Read media that is hosted by this server
 *
 * Media is read from local disk, or, if it has been archived, from S3 using the credentials
 *  of the server (instance role / environment).  It is never read over HTTP.  A request back
 *  to this server for media has to be authenticated and the server has no credentials for
 *  itself, hence server side code that reads media by URL fails whenever authentication is
 *  enabled on the media location.
 *
 * Media paths come from submitted data therefore they are validated to ensure that only
 *  media directories under the base path can be read
 */
public class AttachmentStore {

	private static Logger log = Logger.getLogger(AttachmentStore.class.getName());

	// Directories under the base path that hold media that may be read
	private static final String [] ROOTS = {"attachments/", "media/"};

	// Maximum size of a media file that will be read into memory
	public static final long MAX_MEMORY_BYTES = 50 * 1024 * 1024;

	private AttachmentStore() {
	}

	/*
	 * Get media as a byte array
	 * Returns null if the media is not hosted by this server or it cannot be found
	 */
	public static byte[] getBytes(String basePath, String value, String attachmentPrefix) {
		return getBytesForPath(basePath, getRelativePath(value, attachmentPrefix));
	}

	/*
	 * Get the thumbnail of an image as a byte array, falling back to the full size image
	 * Returns null if the media is not hosted by this server or it cannot be found
	 */
	public static byte[] getThumbnailBytes(String basePath, String value, String attachmentPrefix) {

		byte [] bytes = null;

		String relPath = getRelativePath(value, attachmentPrefix);
		if(relPath != null) {
			bytes = getBytesForPath(basePath, GeneralUtilityMethods.getThumbsUrl(relPath));
			if(bytes == null) {
				bytes = getBytesForPath(basePath, relPath);		// Thumbnail may not have been created
			}
		}

		return bytes;
	}

	/*
	 * Copy media to the specified destination file
	 * Returns false if the media is not hosted by this server or it cannot be found
	 */
	public static boolean copyToFile(String basePath, String value, String attachmentPrefix, File dest) {

		boolean copied = false;

		String relPath = getRelativePath(value, attachmentPrefix);
		if(relPath != null) {
			File src = getLocalFile(basePath, relPath);
			if(src != null) {
				try {
					if(src.exists()) {
						FileUtils.copyFile(src, dest);
						copied = true;
					} else {
						// Media has been archived to S3
						if(dest.getParentFile() != null) {
							dest.getParentFile().mkdirs();
						}
						copied = S3AttachmentUpload.getToFile(basePath, basePath + "/" + relPath, dest);
					}
				} catch (Exception e) {
					log.log(Level.SEVERE, "Error getting media: " + relPath, e);
				}
			}
		}

		return copied;
	}

	/*
	 * Convert a media value, which may be a URL or a path relative to the base path, into a
	 *  path relative to the base path
	 * Returns null if the value does not refer to media hosted by this server
	 */
	public static String getRelativePath(String value, String attachmentPrefix) {

		if(value == null) {
			return null;
		}

		String v = value.trim();

		if(attachmentPrefix != null && attachmentPrefix.length() > 0 && v.startsWith(attachmentPrefix)) {
			v = v.substring(attachmentPrefix.length());
		} else if(v.startsWith("https://") || v.startsWith("http://")) {
			/*
			 * The prefix is not known, or does not match, so look for a media directory in the URL
			 * If there is none then this is not media hosted by this server
			 */
			int idx = -1;
			for(String root : ROOTS) {
				idx = v.indexOf("/" + root);
				if(idx >= 0) {
					v = v.substring(idx + 1);
					break;
				}
			}
			if(idx < 0) {
				return null;
			}
		}

		int qIdx = v.indexOf('?');
		if(qIdx > 0) {
			v = v.substring(0, qIdx);
		}

		return isMediaPath(v) ? v : null;
	}

	/*
	 * Get media, identified by a path relative to the base path, as a byte array
	 */
	private static byte[] getBytesForPath(String basePath, String relPath) {

		byte [] bytes = null;

		if(relPath != null) {
			File f = getLocalFile(basePath, relPath);
			if(f != null) {
				try {
					if(f.exists()) {
						if(f.length() > MAX_MEMORY_BYTES) {
							log.info("Error: media file " + relPath + " of size " + f.length()
									+ " exceeds the maximum size that can be read into memory: " + MAX_MEMORY_BYTES);
						} else {
							bytes = Files.readAllBytes(f.toPath());
						}
					} else {
						// Media has been archived to S3
						bytes = S3AttachmentUpload.getBytes(basePath, basePath + "/" + relPath, MAX_MEMORY_BYTES);
					}
				} catch (Exception e) {
					log.log(Level.SEVERE, "Error getting media: " + relPath, e);
				}
			}
		}

		return bytes;
	}

	/*
	 * Validate that the path is in a media directory and does not attempt to escape from it
	 */
	private static boolean isMediaPath(String relPath) {

		boolean valid = false;

		if(relPath != null && relPath.length() > 0) {
			for(String root : ROOTS) {
				if(relPath.startsWith(root)) {
					valid = true;
					break;
				}
			}
			if(valid && (relPath.contains("..") || relPath.contains("\\") || relPath.contains("\0"))) {
				log.info("Error: invalid media path: " + relPath);
				valid = false;
			}
		}

		return valid;
	}

	/*
	 * Get the local file for a media path after checking that it is contained by the base path
	 * Returns null if the file is outside of the base path, for example via a symbolic link
	 */
	private static File getLocalFile(String basePath, String relPath) {

		File f = new File(basePath + "/" + relPath);
		try {
			String canonicalBase = new File(basePath).getCanonicalPath() + File.separator;
			if(!f.getCanonicalPath().startsWith(canonicalBase)) {
				log.info("Error: media path " + relPath + " is not contained by " + basePath);
				f = null;
			}
		} catch (IOException e) {
			log.log(Level.SEVERE, "Error checking media path: " + relPath, e);
			f = null;
		}

		return f;
	}

}
