package org.smap.sdal.managers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.PdfPageSizer;
import org.smap.sdal.Utilities.PdfUtilities;
import org.smap.sdal.Utilities.QueryGenerator;
import org.smap.sdal.model.DisplayItem;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.QueryForm;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Result;
import org.smap.sdal.model.Row;
import org.smap.sdal.model.ServerData;
import org.smap.sdal.model.SqlDesc;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.User;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.itextpdf.text.Anchor;
import com.itextpdf.text.BadElementException;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Chunk;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.FontFactory;
import com.itextpdf.text.Image;
import com.itextpdf.text.List;
import com.itextpdf.text.ListItem;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.AcroFields;
import com.itextpdf.text.pdf.BarcodeQRCode;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.text.pdf.PushbuttonField;
import com.itextpdf.tool.xml.ElementList;
import com.itextpdf.tool.xml.XMLWorker;
import com.itextpdf.tool.xml.XMLWorkerFontProvider;
import com.itextpdf.tool.xml.XMLWorkerHelper;
import com.itextpdf.tool.xml.css.CssFile;
import com.itextpdf.tool.xml.css.StyleAttrCSSResolver;
import com.itextpdf.tool.xml.html.CssAppliers;
import com.itextpdf.tool.xml.html.CssAppliersImpl;
import com.itextpdf.tool.xml.html.Tags;
import com.itextpdf.tool.xml.parser.XMLParser;
import com.itextpdf.tool.xml.pipeline.css.CSSResolver;
import com.itextpdf.tool.xml.pipeline.css.CssResolverPipeline;
import com.itextpdf.tool.xml.pipeline.end.ElementHandlerPipeline;
import com.itextpdf.tool.xml.pipeline.html.HtmlPipeline;
import com.itextpdf.tool.xml.pipeline.html.HtmlPipelineContext;

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

public class PDFReportsManager {

	private static Logger log =
			Logger.getLogger(PDFReportsManager.class.getName());

	LogManager lm = new LogManager();		// Application log

	// Global values set in constructor
	private ResourceBundle localisation;
	private Survey survey;
	private Connection sd;
	
	// Other global values
	int languageIdx = 0;
	int utcOffset = 0;

	boolean mExcludeEmpty = false;
	

	
	public PDFReportsManager(ResourceBundle l, Connection c) {
		localisation = l;
		sd = c;
	}
	
	/*
	 * Create the new style XLSX report
	 */
	public Response getNewReport(
			Connection sd,
			Connection cResults,
			String username,
			HttpServletRequest request,
			HttpServletResponse response,
			int sId, 
			String filename, 
			boolean landscape, 
			String language,
			Date startDate,
			Date endDate,
			int dateId,
			String filter,
			boolean meta) throws Exception {
		
		Response responseVal = null;
		String basePath = GeneralUtilityMethods.getBasePath(request);
		SurveyManager sm = new SurveyManager(localisation);
		PreparedStatement pstmt = null;
		
		try {
					
			/*
			 * Get the sql
			 */
			Form f = GeneralUtilityMethods.getTopLevelForm(sd, sId);
			QueryManager qm = new QueryManager();	
			ArrayList<QueryForm> queryList = null;
			queryList = qm.getFormList(sd, sId, f.id);		// Get a form list for this survey / form combo

			QueryForm startingForm = qm.getQueryTree(sd, queryList);	// Convert the query list into a tree
			String urlprefix = request.getScheme() + "://" + request.getServerName() + "/";	
			SqlDesc sqlDesc = QueryGenerator.gen(
					sd, 
					cResults,
					localisation,
					sId,
					f.id,
					language, 
					"pdf", 
					urlprefix,
					true,
					true,
					false,
					null,
					false,
					true,
					request.getServerName().toLowerCase(),
					null,
					null,
					request.getRemoteUser(),
					startDate,
					endDate,
					dateId,
					false,			// superUser - Always apply filters
					startingForm,
					filter,
					true,
					true);		// Just get the instanceId
			
			pstmt = cResults.prepareStatement(sqlDesc.sql);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String instanceId = rs.getString("instanceid");
	 			Survey survey = sm.getById(sd, cResults, username, sId, true, basePath, 
						instanceId, true, false, true, false, true, "real", 
						false, false, true, "geojson");
				
				PDFSurveyManager pm = new PDFSurveyManager(localisation, sd, survey);
				
				System.out.println("Instance Id: " + instanceId);
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
		
		return responseVal;
	}


}


