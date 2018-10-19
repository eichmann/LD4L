package edu.uiowa.slis.LD4L.Sinopia;

public class PropertyTemplate {
    String label = null;
    String remark = null;
    String URI = null;
    String type = null;
    boolean repeatable = true;
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

    public String getURI() {
        return URI;
    }

    public void setURI(String uRI) {
        URI = uRI;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isRepeatable() {
        return repeatable;
    }

    public void setRepeatable(boolean repeatable) {
        this.repeatable = repeatable;
    }

    public boolean isMandatory() {
        return mandatory;
    }

    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }

    public ValueConstraint getValueConstraint() {
        return valueConstraint;
    }

}
