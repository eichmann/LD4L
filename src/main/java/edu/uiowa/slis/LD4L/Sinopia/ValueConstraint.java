package edu.uiowa.slis.LD4L.Sinopia;

import java.util.Vector;

public class ValueConstraint {
    boolean editable = false;
    String language = null;
    String languageURI = null;
    String languageLabel = null;
    String remark = null;
    String defaultURI = null;
    String defaultLiteral = null;
    String dataTypeURI = null;
    Vector<String> valueTemplateRefs = new Vector<String>();
    Vector<String> useValuesFrom = new Vector<String>();
    
    public ValueConstraint(boolean editable, String language, String languageURI, String languageLabel, String remark, String defaultURI, String defaultLiteral, String dataTypeURI) {
	this.editable = editable;
	this.language = language;
	this.languageURI = languageURI;
	this.languageLabel = languageLabel;
	this.remark = remark;
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

    public boolean isEditable() {
        return editable;
    }

    public void setEditable(boolean editable) {
        this.editable = editable;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getLanguageURI() {
        return languageURI;
    }

    public void setLanguageURI(String languageURI) {
        this.languageURI = languageURI;
    }

    public String getLanguageLabel() {
        return languageLabel;
    }

    public void setLanguageLabel(String languageLabel) {
        this.languageLabel = languageLabel;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getDefaultURI() {
        return defaultURI;
    }

    public void setDefaultURI(String defaultURI) {
        this.defaultURI = defaultURI;
    }

    public String getDefaultLiteral() {
        return defaultLiteral;
    }

    public void setDefaultLiteral(String defaultLiteral) {
        this.defaultLiteral = defaultLiteral;
    }

    public String getDataTypeURI() {
        return dataTypeURI;
    }

    public void setDataTypeURI(String dataTypeURI) {
        this.dataTypeURI = dataTypeURI;
    }

    public Vector<String> getValueTemplateRefs() {
        return valueTemplateRefs;
    }

    public void setValueTemplateRefs(Vector<String> valueTemplateRefs) {
        this.valueTemplateRefs = valueTemplateRefs;
    }

    public Vector<String> getUseValuesFrom() {
        return useValuesFrom;
    }

    public void setUseValuesFrom(Vector<String> useValuesFrom) {
        this.useValuesFrom = useValuesFrom;
    }
}
