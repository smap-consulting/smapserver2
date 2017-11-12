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

package org.smap.subscribers;

import java.io.InputStream;
import java.util.Date;
import java.util.ResourceBundle;

import org.smap.model.SurveyInstance;
import org.smap.server.entities.SubscriberEvent;


public class DocumentSync extends Subscriber {


	/**
	 * @param args
	 */
	public DocumentSync() {
		super();

	}

	@Override
	public String getDest() {
		return sNameRemote + " @ " + host;

	}

	@Override
	public void upload(SurveyInstance instance, InputStream xis, String remoteUser, 
			String server, String device, SubscriberEvent se, String confFilePath, String formStatus,
			String basePath, String filePath, String updateId, int ue_id, Date uploadTime,
			String surveyNotes, String locationTrigger, String auditFilePath, ResourceBundle l) {


		localisation  = l;
		System.out.println("++++++++++++++ Document Synchronisation");

	}



}
