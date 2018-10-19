package edu.uiowa.slis.LD4L.Sinopia;

import java.util.Date;
import java.util.Vector;

public class Profile {
    String id = null;
    Date modificationDate = null;
    Vector<ResourceTemplate> resourceTemplates = new Vector<ResourceTemplate>();
    
    public Profile(String id, Date modificationDate) {
	this.id = id;
	this.modificationDate = modificationDate;
    }
    
    public void addResourceTemplate(ResourceTemplate template) {
	resourceTemplates.addElement(template);
    }

}
