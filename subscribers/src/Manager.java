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
		 * Start asynchronous worker threads in upload processor
		 * 1. Message processor
		 */
		if(subscriberType.equals("upload")) {
			MessageProcessor mp = new MessageProcessor();
			mp.go(smapId, fileLocn);

			// Start the AWS service processor
			String mediaBucket = GeneralUtilityMethods.getSettingFromFile("/home/ubuntu/bucket");
			String region = GeneralUtilityMethods.getSettingFromFile("/home/ubuntu/region");
			System.out.println("Auto Update:  S3 Bucket is: " + region + " : " + mediaBucket);
			
			AutoUpdateProcessor au = new AutoUpdateProcessor();
			au.go(smapId, fileLocn, mediaBucket, region);
		}
		
		System.out.println("Starting prop subscriber: " + smapId + " : " + fileLocn + " : " + subscriberType);
		int delaySecs = 4;
		
		// Forwarding can happen less frequently, this reduce the load due to searching for items to forward
		if(subscriberType.equals("forward")) {
			delaySecs = 30;					
		}
		
		boolean loop = true;
		while(loop) {

			SubscriberBatch batchJob = new SubscriberBatch();
			batchJob.go(smapId, fileLocn, subscriberType);	// Run the batch job for the specified server

			String subscriberControl = GeneralUtilityMethods.getSettingFromFile("/home/ubuntu/subscriber");
			if(subscriberControl != null && subscriberControl.equals("stop")) {
				System.out.println("######## Stopped");
				loop = false;
			} else {
			
				try {
					Thread.sleep(delaySecs * 1000);
				} catch (Exception e) {
					// ignore
				}
			}

		}
				
	}
	
	


}
