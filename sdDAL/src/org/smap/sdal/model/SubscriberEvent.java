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

package org.smap.sdal.model;

public class SubscriberEvent {

	
	private String status = null;
	
	private String reason = null;

	public SubscriberEvent() {
		
	}

	/*
	 * Getters
	 */
	
	public String getStatus() {
		return status;
	}
	
	public String getReason() {
		return reason;
	}
 
	/*
	 * Setters
	 */
    
	public void setStatus(String value) {
		status = value;
	}
	
	public void setReason(String value) {
		reason = value;
	}

    

}
