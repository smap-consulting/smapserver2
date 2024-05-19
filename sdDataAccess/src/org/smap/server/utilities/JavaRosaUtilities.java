package org.smap.server.utilities;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import javax.servlet.http.HttpServletRequest;

import org.javarosa.core.model.condition.IFunctionHandler;
import org.javarosa.core.model.data.IAnswerData;
import org.javarosa.core.model.instance.InstanceInitializationFactory;
import org.javarosa.core.model.instance.TreeElement;
import org.javarosa.core.model.utils.IPreloadHandler;
import org.javarosa.model.xform.XFormsModule;
import org.javarosa.xform.util.XFormUtils;
import org.smap.model.SurveyTemplate;

public class JavaRosaUtilities {

    /*
     * Validate a survey stored in the database using the javarosa api
     * Will throw an exception on errors
     */
    public static void javaRosaSurveyValidation(ResourceBundle localisation, int sId, String user, String tz, HttpServletRequest request) throws Exception {
		
    		class FakePreloadHandler implements IPreloadHandler {

            String preloadHandled;


            public FakePreloadHandler(String preloadHandled) {
                this.preloadHandled = preloadHandled;
            }


            public boolean handlePostProcess(TreeElement arg0, String arg1) {
                // TODO Auto-generated method stub
                return false;
            }


            public IAnswerData handlePreload(String arg0) {
                // TODO Auto-generated method stub
                return null;
            }


            public String preloadHandled() {
                // TODO Auto-generated method stub
                return preloadHandled;
            }

        }
		new XFormsModule().registerModule();
		
		SurveyTemplate template = new SurveyTemplate(localisation);
		template.readDatabase(sId, false);
		GetXForm xForm = new GetXForm(localisation, user, tz);

		String xmlForm = xForm.get(template, false, true, false, user, request);
		
		// Remove any actions
		xmlForm = xmlForm.replaceAll("\\<odk:setgeopoint [a-zA-Z0-9$,\\\\.{}=\\'\\-\"/ ]*\\/\\>", "");	
		xmlForm = xmlForm.replaceAll("\\<odk:recordaudio .*\\/\\>", "");
		
		//FileWriter myWriter = new FileWriter("/Users/neilpenman/filename.xml");  // Debug enable if parser reports an error in the xml
		//myWriter.write(xmlForm);
		//myWriter.close();
		
		InputStream is = new ByteArrayInputStream(xmlForm.getBytes());

		org.javarosa.core.model.FormDef fd = XFormUtils.getFormFromInputStream(is);

		// make sure properties get loaded
		fd.getPreloader().addPreloadHandler(new FakePreloadHandler("property"));

		// update evaluation context for function handlers
		addHandler(fd, "pulldata");
		addHandler(fd, "lookup_image_labels");
		addHandler(fd, "get_media");
		addHandler(fd, "lookup");
		addHandler(fd, "lookup_choices");

		fd.initialize(false, new InstanceInitializationFactory());

	}
    
    private static void addHandler(org.javarosa.core.model.FormDef fd, String name) {
    	
    	fd.getEvaluationContext().addFunctionHandler(new IFunctionHandler() {

			public String getName() {
				return name;
			}

			public List<Class[]> getPrototypes() {
				return new ArrayList<Class[]>();
			}

			public boolean rawArgs() {
				return true;
			}

			public boolean realTime() {
				return false;
			}

			@Override
			public Object eval(Object[] arg0, org.javarosa.core.model.condition.EvaluationContext arg1) {
				// TODO Auto-generated method stub
				//return arg0[0];
				return "";
			}
		});
    }
}
