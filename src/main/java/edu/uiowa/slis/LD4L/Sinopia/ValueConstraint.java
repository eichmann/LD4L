package edu.uiowa.slis.LD4L.Sinopia;

import java.util.Vector;

public class ValueConstraint {
    boolean editable = false;
    boolean repeatable = false;
    String defaultURI = null;
    String defaultLiteral = null;
    String dataTypeURI = null;
    Vector<String> valueTemplateRefs = new Vector<String>();
    Vector<String> useValuesFrom = new Vector<String>();
    
    public ValueConstraint(boolean editable, boolean repeatable, String defaultURI, String defaultLiteral, String dataTypeURI) {
	this.editable = editable;
	this.repeatable = repeatable;
	this.defaultURI = defaultURI;
	this.defaultLiteral = defaultLiteral;
	this.dataTypeURI = dataTypeURI;
    }
    
    public void addValueTemplateRef(String valueTemplateRef) {
	valueTemplateRefs.addElement(valueTemplateRef);
    }
    
    public void addUseValeFrom(String useValueFrom) {
	useValuesFrom.addElement(useValueFrom);
    }
}
