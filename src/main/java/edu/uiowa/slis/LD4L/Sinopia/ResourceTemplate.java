package edu.uiowa.slis.LD4L.Sinopia;

import java.util.Vector;

public class ResourceTemplate {
    String id = null;
    String URI = null;
    String label = null;
    String remark = null;
    Vector<PropertyTemplate> propertyTemplates = new Vector<PropertyTemplate>();
    
    public ResourceTemplate(String id, String URI, String label, String remark) {
	this.id = id;
	this.URI = URI;
	this.label = label;
	this.remark = remark;
    }
    
    public void addPropertyTemplate(PropertyTemplate template) {
	propertyTemplates.addElement(template);
    }
    
    public boolean hasRepeatableProperty() {
	for (PropertyTemplate property : propertyTemplates) {
	    if (property.isRepeatable())
		return true;
	}
	return false;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getURI() {
        return URI;
    }

    public void setURI(String uRI) {
        URI = uRI;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Vector<PropertyTemplate> getPropertyTemplates() {
        return propertyTemplates;
    }

    public void setPropertyTemplates(Vector<PropertyTemplate> propertyTemplates) {
        this.propertyTemplates = propertyTemplates;
    }

}
