package edu.uiowa.slis.LD4L.Sinopia;

import java.util.Vector;

public class ResourceTemplate {
    String id = null;
    String URI = null;
    String label = null;
    Vector<PropertyTemplate> propertyTemplates = new Vector<PropertyTemplate>();
    
    public ResourceTemplate(String id, String URI, String label) {
	this.id = id;
	this.URI = URI;
	this.label = label;
    }
    
    public void addPropertyTemplate(PropertyTemplate template) {
	propertyTemplates.addElement(template);
    }

}
