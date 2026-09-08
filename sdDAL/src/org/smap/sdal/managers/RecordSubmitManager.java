package org.smap.sdal.managers;

import java.io.File;
import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.legacy.SurveyTemplate;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.UploadEvent;

/*
 * Create a submission from values rather than from a filled in form.
 *
 * Smap has never had this: every record arrives as instance XML from a device or a webform, and the
 * console edits one by opening the form again. So rather than write a second way into the results
 * tables, this builds the same XML the form would have produced and hands it to the same pipeline.
 *
 * That matters more than the effort it costs. A record inserted straight into the results database
 * would skip the upload event, the queue, the duplicate check, the human readable key policy, the
 * notifications and the tasks - everything that makes a submission a submission rather than a row.
 * It would look identical until the day somebody asked why an MCP record never triggered an alert.
 *
 * The work is therefore deliberately narrow: build an Instance, ask GetXForm for the XML it implies,
 * write it where an upload would have been written, and record the upload event. The subscriber does
 * the rest, on its own schedule, exactly as it does for a phone.
 */
public class RecordSubmitManager {

	private static Logger log = Logger.getLogger(RecordSubmitManager.class.getName());

	/* The name the meta question carries in the form definition, and so the key an answer needs */
	private static final String INSTANCE_ID = "instanceID";

	private ResourceBundle localisation;
	private String tz;

	public RecordSubmitManager(ResourceBundle l, String tz) {
		this.localisation = l;
		this.tz = tz == null ? "UTC" : tz;
	}

	/* What was accepted, and where it went */
	public static class Submitted {
		public String instanceId;
		public String filePath;
	}

	/*
	 * Queue a new record. It is not in the results database when this returns, and deliberately so:
	 * a submission is applied by the subscriber, and pretending otherwise would be a different
	 * promise from the one every other submission makes.
	 */
	public Submitted submit(Connection sd, Connection cResults, Survey survey,
			Map<String, String> values, String user, String agent, String basePath,
			String serverName) throws Exception {

		String templateName = survey.getIdent();

		/*
		 * The XML the form would have produced.
		 *
		 * createBlank asks for the skeleton of an empty record, and the Instance supplies the
		 * answers, so the shape comes from the survey definition rather than from anything assumed
		 * here about how a question is spelled.
		 */
		SurveyTemplate template = new SurveyTemplate(localisation);
		template.readDatabase(sd, cResults, templateName, false);

		/*
		 * The instance id is generated here and supplied as an answer, rather than read back out of
		 * the finished document.
		 *
		 * A blank instance carries an empty instanceID, because normally the device filling in the
		 * form is the thing that invents one. Nothing else does it on this path, so this is the
		 * device. Written in the same shape every other record uses, so a record submitted this way
		 * is indistinguishable from one submitted by a phone.
		 */
		String instanceId = "uuid:" + UUID.randomUUID();

		Instance initialData = new Instance();
		initialData.values.putAll(values);
		initialData.values.put(INSTANCE_ID, instanceId);

		GetXForm xForm = new GetXForm(localisation, user, tz);
		String instanceXml = xForm.getInstanceXml(survey.getId(), templateName, template,
				null,			// key
				null,			// key value
				0,				// prikey
				false,			// simplifyMedia
				false,			// isWebForms
				0,				// taskKey
				null,			// url prefix: no media is being referenced
				initialData,
				true);			// createBlank

		if(instanceXml == null || instanceXml.trim().isEmpty()) {
			throw new ApplicationException("The survey definition could not be turned into a record");
		}

		/*
		 * Written where an upload would have been written, under a fresh identifier.
		 *
		 * The folder and the file share a name because that is the convention the rest of the
		 * pipeline expects, and both are UUIDs so that two submissions of the same form cannot
		 * collide.
		 */
		String instanceDir = String.valueOf(UUID.randomUUID());
		String surveyPath = basePath + "/uploadedSurveys/" + templateName;
		String instancePath = surveyPath + "/" + instanceDir;
		FileUtils.forceMkdir(new File(instancePath));

		String fileName = instanceDir + ".xml";
		String filePath = instancePath + "/" + fileName;
		FileUtils.writeStringToFile(new File(filePath), instanceXml, "UTF-8");

		/*
		 * Confirmed rather than assumed. The upload event and the document have to name the same
		 * record: the subscriber reads the id out of the file, so an event naming a different one
		 * would leave a record nothing could find.
		 */
		if(!instanceXml.contains(instanceId)) {
			throw new ApplicationException("The record was built without its instance id");
		}

		UploadEvent ue = new UploadEvent();
		ue.setUserName(user);
		ue.setServerName(serverName);
		ue.setSurveyId(survey.getId());
		ue.setIdent(templateName);
		ue.setFilePath(filePath);
		ue.setFileName(fileName);
		ue.setOrigSurveyIdent(fileName);
		ue.setProjectId(survey.getPId());
		ue.setOrganisationId(survey.surveyData.o_id);
		ue.setEnterpriseId(survey.surveyData.e_id);
		ue.setUploadTime(new Date());
		ue.setSurveyName(survey.getDisplayName());
		ue.setInstanceId(instanceId);
		ue.setStatus("success");
		ue.setIncomplete(false);
		ue.setFormStatus("complete");
		ue.setTemporaryUser(false);
		/*
		 * What submitted it, in both senses the pipeline understands. The device column is what a
		 * person reading the data sees beside "webform" or "Android"; the agent is what reaches the
		 * record's history, and is the one that says a program acted for somebody.
		 */
		ue.setImei(agent);
		ue.setAgent(agent);

		UploadEventManager uem = new UploadEventManager(sd);
		try {
			uem.write(ue, false);		// not yet applied: the subscriber does that
		} finally {
			uem.close();
		}

		log.info("MCP submission queued for " + templateName + ", instance " + instanceId);

		Submitted submitted = new Submitted();
		submitted.instanceId = instanceId;
		submitted.filePath = filePath;
		return submitted;
	}


}
