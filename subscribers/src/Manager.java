import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

/*****************************************************************************

This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

 ******************************************************************************/

/*
 * Usage java -jar subscribers.jar {path to subscriber configurations} {file base path} 
 */

public class Manager {
	
	private static Logger log =
			 Logger.getLogger(Manager.class.getName());
	
	public static void main(String[] args) {
		
		String fileLocn = "/smap";			// Default for legacy servers that do not set file path
		String subscriberType = "upload";	// Default subscriberType
		
		String smapId = args[0];
		if(args.length > 1) {
			if(args[1] != null && !args[1].equals("null")) {
				fileLocn = args[1];	
			}
		}
		if(args.length > 2) {
			subscriberType = args[2];	
		}
		
		/*
		 * Start asynchronous worker threads
		 */
		if(subscriberType.equals("forward")) {
			
			// Start the AWS service processor
			String mediaBucket = GeneralUtilityMethods.getSettingFromFile(fileLocn + "/settings/bucket");
			String region = GeneralUtilityMethods.getSettingFromFile(fileLocn + "/settings/region");
			log.info("Auto Update:  S3 Bucket is: " + region + " : " + mediaBucket);
			
			AutoUpdateProcessor au = new AutoUpdateProcessor();
			au.go(smapId, fileLocn, mediaBucket, region);
			
			/*
			 * Start the message processor
			 */
			MessageProcessor mp = new MessageProcessor();
			mp.go(smapId, fileLocn);
			
			/*
			 * Start the storage processor - required if images are stored on s3
			 */
			StorageProcessor sp = new StorageProcessor();
			sp.go(smapId, fileLocn);
			
			/*
			 * Start the report processor
			 */
			ReportProcessor rp = new ReportProcessor();
			rp.go(smapId, fileLocn);
			
			/*
			 * Start the submission event processor
			 */
			SubEventProcessor sep = new SubEventProcessor();
			sep.go(smapId, fileLocn);
		}
		
		log.info("Starting prop subscriber: " + smapId + " : " + fileLocn + " : " + subscriberType);
		int delaySecs = 2;
		
		// The forward batch job processes events to reduce the server load
		if(subscriberType.equals("forward")) {
			delaySecs = 30;					
		}
		
		SubscriberBatch batchJob = new SubscriberBatch();
		boolean loop = true;
		while(loop) {
			String subscriberControl = GeneralUtilityMethods.getSettingFromFile(fileLocn + "/settings/subscriber");
			if(subscriberControl != null && subscriberControl.equals("stop")) {
				log.info("######## Stopped");		
				loop = false;
			} else {
				System.out.print("-");	// Log running of batch job
				batchJob.go(smapId, fileLocn, subscriberType);	// Run the batch job for the specified server

				try {
					Thread.sleep(delaySecs * 1000);
				} catch (Exception e) {
					// ignore
				}
			}

		}
				
	}
	
}
