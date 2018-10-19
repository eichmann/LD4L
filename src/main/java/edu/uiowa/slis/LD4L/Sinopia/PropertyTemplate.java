package edu.uiowa.slis.LD4L.Sinopia;

public class PropertyTemplate {
    String label = null;
    String remark = null;
    String URI = null;
    String type = null;
    boolean repeatable = false;
    boolean mandatory = false;
    ValueConstraint valueConstraint = null;
    
    public PropertyTemplate(String label, String remark, String URI, String type, boolean repeatable, boolean mandatory) {
	this.label = label;
	this.remark = remark;
	this.URI = URI;
	this.type = type;
	this.repeatable = repeatable;
	this.mandatory = mandatory;
    }
    
    public void setValueConstraint(ValueConstraint valueConstraint) {
	this.valueConstraint = valueConstraint;
    }

}
