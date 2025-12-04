package org.smap.sdal.model;

import java.util.ArrayList;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

@XmlRootElement(name="xforms")
@XmlSeeAlso({ODKForm.class})
public class XformsJavaRosa {
	
	//@XmlAttribute
	//public String xmlns = "http://openrosa.org/xforms/xformsList";
	
	public ArrayList<ODKForm> xform;
	
}
