package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

import com.vonage.jwt.Jwt;


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
 * This class supports JWT validation
 * At least it does when the message is an SMS message from Vonage
 */
public class JWTManager {
	
	private static Logger log = Logger.getLogger(JWTManager.class.getName());

	LogManager lm = new LogManager(); // Application log
	
	
	public JWTManager() {
		
	}
	
	public boolean validate(Connection sd, String key) throws SQLException {
		boolean isValid = false;
		if(key != null) {
			if(key.startsWith("Bearer")) {
				String[] comps = key.split(" ");
				if(comps.length == 2) {
					String token = comps[1];
					
					 if (Jwt.verifySignature(token, GeneralUtilityMethods.getVonageWebHookSecret(sd))) {
						 isValid = true;
					 } else {
						 log.info("Error: JWT: Invalid key: Signature validation failed" + key);
					 }	
					
				} else {
					log.info("Error: JWT: Invalid key does not have two parts separated by a space: " + key);
				}
			} else {
				log.info("Error: JWT: Invalid key does not start with Bearer: " + key);
			}
		} else {
			isValid = true;		// Signed webhooks not being used
			log.info("Error: ********************************************** JWT: Key is null");
		}
		return isValid;
	}

}
