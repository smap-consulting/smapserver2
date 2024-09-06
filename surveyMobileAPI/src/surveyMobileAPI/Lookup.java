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

package surveyMobileAPI;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.managers.LookupManager;


/*
 * Requests for realtime data from a form
 */

@Path("/lookup")
public class Lookup extends Application{	
	
	/*
	 * Get a record from the reference data identified by the filename and key column
	 */
	@GET
	@Path("/{survey_ident}/{filename}/{key_column}/{key_value}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response lookup(@Context HttpServletRequest request,
			@PathParam("survey_ident") String surveyIdent,		// Survey that needs to lookup some data
			@PathParam("filename") String fileName,				// CSV filename, could be the identifier of another survey
			@PathParam("key_column") String keyColumn,
			@PathParam("key_value") String keyValue,
			@QueryParam("index") String indexFn,
			@QueryParam("searchType") String searchType,
			@QueryParam("expression") String expression
			) throws IOException, ApplicationException {

		LookupManager lm = new LookupManager();
		return lm.lookup(request, surveyIdent, fileName, keyColumn, 
				keyValue, indexFn, searchType, expression);
	}
	
	/*
	 * Get external choices
	 */
	@GET
	@Path("/choices/{survey_ident}/{filename}/{value_column}/{label_columns}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response choices(@Context HttpServletRequest request,
			@PathParam("survey_ident") String surveyIdent,		// Survey that needs to lookup some data
			@PathParam("filename") String fileName,				// CSV filename, could be the identifier of another survey
			@PathParam("value_column") String valueColumn,
			@PathParam("label_columns") String labelColumns,
			@QueryParam("search_type") String searchType,
			@QueryParam("q_column") String qColumn,
			@QueryParam("q_value") String qValue,
			@QueryParam("f_column") String fColumn,
			@QueryParam("f_value") String fValue,
			@QueryParam("expression") String expression
			) throws IOException, ApplicationException {

		LookupManager lm = new LookupManager();
		return lm.choices(request, surveyIdent, fileName, valueColumn, 
				labelColumns, searchType, qColumn, qValue, fColumn, fValue,
				expression);	
	}
	
	/*
	 * Get external choices (Multi Language Version)
	 */
	@GET
	@Path("/mlchoices/{survey_ident}/{filename}/{question}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response mlchoices(@Context HttpServletRequest request,
			@PathParam("survey_ident") String surveyIdent,		// Survey that needs to lookup some data
			@PathParam("filename") String fileName,				// CSV filename, could be the identifier of another survey
			@PathParam("question") String questionName,
			@QueryParam("search_type") String searchType,
			@QueryParam("q_column") String qColumn,
			@QueryParam("q_value") String qValue,
			@QueryParam("f_column") String fColumn,
			@QueryParam("f_value") String fValue,
			@QueryParam("expression") String expression	
			) throws IOException, ApplicationException {

		LookupManager lm = new LookupManager();
		return lm.mlchoices(request, surveyIdent, fileName, questionName, 
				searchType, qColumn, qValue, fColumn, fValue,
				expression);
	}

	/*
	 * Get labels from an image
	 */
	@POST
	@Path("/imagelabels/{survey_ident}")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.APPLICATION_JSON)
	public Response imageLookup(@Context HttpServletRequest request,
			@PathParam("survey_ident") String surveyIdent		// Survey that needs to lookup image label
			) throws IOException {
		
		LookupManager lm = new LookupManager();
		return lm.imageLookup(request, surveyIdent);
	}	
}

