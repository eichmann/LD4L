package edu.uiowa.slis.LD4L.Sinopia;

import java.util.Date;
import java.util.Vector;

public class Profile {
    String id = null;
    String title = null;
    String description = null;
    String contact = null;
    String remark = null;
    Date modificationDate = null;
    Vector<ResourceTemplate> resourceTemplates = new Vector<ResourceTemplate>();
    
    public Profile(String id, String title, String description, String contact, String remark, Date modificationDate) {
	this.id = id;
	this.title = title;
	this.description = description;
	this.contact = contact;
	this.remark = remark;
	this.modificationDate = modificationDate;
    }
    
    public void addResourceTemplate(ResourceTemplate template) {
	resourceTemplates.addElement(template);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getContact() {
        return contact;
    }

    public void setContact(String contact) {
        this.contact = contact;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Date getModificationDate() {
        return modificationDate;
    }

    public void setModificationDate(Date modificationDate) {
        this.modificationDate = modificationDate;
    }

    public Vector<ResourceTemplate> getResourceTemplates() {
        return resourceTemplates;
    }

    public void setResourceTemplates(Vector<ResourceTemplate> resourceTemplates) {
        this.resourceTemplates = resourceTemplates;
    }

}
