package edu.uiowa.slis.LD4L;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import edu.uiowa.slis.LD4L.Sinopia.Profile;
import edu.uiowa.slis.LD4L.Sinopia.PropertyTemplate;
import edu.uiowa.slis.LD4L.Sinopia.ResourceTemplate;
import edu.uiowa.slis.LD4L.Sinopia.ValueConstraint;

public class SinopiaProfileLoader {
    static Logger logger = Logger.getLogger(SinopiaProfileLoader.class);
    static final String networkHostName = "localhost";
    static Connection conn = null;
    static String thePath = "/Users/eichmann/downloads/sinopia_sample_profiles-master/verso";
    static Hashtable<String, Profile> profileHash = new Hashtable<String, Profile>();
    static Hashtable<String, ResourceTemplate> resourceHash = new Hashtable<String, ResourceTemplate>();

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
	PropertyConfigurator.configure(args[0]);
	logger.info("connecting to database...");
	Class.forName("org.postgresql.Driver");
	Properties props = new Properties();
	props.setProperty("user", "eichmann");
	props.setProperty("password", "translational");
	// props.setProperty("sslfactory",
	// "org.postgresql.ssl.NonValidatingFactory");
	// props.setProperty("ssl", "true");
	conn = DriverManager.getConnection("jdbc:postgresql://" + networkHostName + "/ld4l", props);

	simpleStmt("truncate profiles.profile");
	simpleStmt("truncate profiles.sparql");
	
	File theFile = new File(thePath);

	if (theFile.isDirectory()) {
	    File[] theFiles = theFile.listFiles();
	    for (int i = 0; i < theFiles.length; i++)
		processFile(theFiles[i]);
	} else {
	    processFile(theFile);
	}
	
	processProfiles();
	generateQuery("profile:bf2:Monograph:Work", "<http://share-vde.org/sharevde/rdfBibframe2/Work/18724624>");
    }

    static void processFile(File theFile) throws IOException, SQLException {
	logger.info("file:" + theFile.getName());
	FileReader theFileReader = new FileReader(theFile);
	BufferedReader reader = new BufferedReader(theFileReader);
	JSONObject theObject = (new JSONObject(new JSONTokener(reader))).getJSONObject("Profile");

	String id = theObject.getString("id").trim();
	String title = theObject.getString("title").trim();
	String description = theObject.getString("description").trim();
	String date = theObject.getString("date").trim();

	logger.info("profile: " + id);
	logger.info("\ttitle: " + title);
	logger.info("\tdescription: " + description);
	logger.info("\tdate: " + date);

	PreparedStatement connStmt = conn.prepareStatement("insert into profiles.profile(id,title,description,modification_date,profile) values(?,?,?,?::date,?::jsonb)");
	connStmt.setString(1, id);
	connStmt.setString(2, title);
	connStmt.setString(3, description);
	connStmt.setString(4, date);
	connStmt.setString(5, theObject.toString());
	connStmt.execute();
	connStmt.close();
    }
    
    static void processProfiles() throws SQLException {
	PreparedStatement connStmt = conn.prepareStatement("select id,modification_date,profile from profiles.profile where id not in (select id from profiles.sparql) order by id");
	ResultSet rs = connStmt.executeQuery();
	while (rs.next()) {
	    String id = rs.getString(1);
	    Date modDate = rs.getDate(2);
	    JSONObject profile = new JSONObject(new JSONTokener(new StringReader(rs.getString(3))));
	    String title = profile.getString("title").trim();
	    String description = profile.getString("description").trim();
	    String contact = profile.optString("contact");
	    String remark = profile.optString("remark");
	    Profile theProfile = new Profile(id, title, description, contact, remark, modDate);
	    profileHash.put(id, theProfile);
	    
	    logger.info("parsing profile: " + id);
	    logger.trace(profile.toString(3));
	    
	    JSONArray resourceTemplates = profile.getJSONArray("resourceTemplates");
	    for (int i = 0; i < resourceTemplates.length(); i++) {
		JSONObject resourceTemplate = resourceTemplates.getJSONObject(i);
		String resourceID = resourceTemplate.getString("id");
		String resourceURI = resourceTemplate.getString("resourceURI");
		String resourceLabel = resourceTemplate.getString("resourceLabel");
		String resourceRemark = resourceTemplate.optString("remark");
		
		logger.info("\tresourceID: " + resourceID);
		logger.info("\t\tresourceURI: " + resourceURI);
		logger.info("\t\tresourceLabel: " + resourceLabel);
		logger.info("\t\tresourceRemark: " + resourceRemark);
		
		ResourceTemplate theResourceTemplate = new ResourceTemplate(resourceID, resourceURI, resourceLabel, resourceRemark);
		theProfile.addResourceTemplate(theResourceTemplate);
		resourceHash.put(resourceID, theResourceTemplate);
		
		JSONArray propertyTemplates = resourceTemplate.getJSONArray("propertyTemplates");
		for (int j = 0; j < propertyTemplates.length(); j++) {
		    JSONObject propertyTemplate = propertyTemplates.getJSONObject(j);
		    String propertyRemark = propertyTemplate.optString("remark");
		    String propertyLabel = propertyTemplate.getString("propertyLabel");
		    boolean repeatable = propertyTemplate.optBoolean("repeatable", true);
		    boolean mandatory = propertyTemplate.optBoolean("mendatory", false);
		    String type = propertyTemplate.getString("type");
		    String propertyURI = propertyTemplate.getString("propertyURI");

		    logger.info("\t\tpropertyURI: " + propertyURI);
		    logger.info("\t\t\tpropertyLabel: " + propertyLabel);
		    logger.info("\t\t\ttype: " + type);
		    logger.info("\t\t\trepeatable: " + repeatable);
		    logger.info("\t\t\tmandatory: " + mandatory);
		    logger.info("\t\t\tpropertyRemark: " + propertyRemark); 
		    
		    PropertyTemplate thePropertyTemplate = new PropertyTemplate(propertyLabel, remark, propertyURI, type, repeatable, mandatory);
		    theResourceTemplate.addPropertyTemplate(thePropertyTemplate);
		    
		    JSONObject valueConstraint = propertyTemplate.getJSONObject("valueConstraint");
		    logger.trace(valueConstraint.toString(1));
		    boolean valueEditable = valueConstraint.optBoolean("editable", false);
		    String language = valueConstraint.optString("language");
		    String languageURI = valueConstraint.optString("languageURI");
		    String languageLabel = valueConstraint.optString("languageLabel");
		    String valueConstraintRemark = valueConstraint.optString("remark");
		    String defaultURI = valueConstraint.optString("defaultURI");
		    String defaultLiteral = valueConstraint.optString("defaultLiteral");
		    String dataTypeURI = valueConstraint.optJSONObject("valueDataType") == null ? null : valueConstraint.getJSONObject("valueDataType").optString("dataTypeURI");
		    logger.info("\t\t\tvalueEditable: " + valueEditable);
		    logger.info("\t\t\tlanguage: " + language);
		    logger.info("\t\t\tlanguageURI: " + languageURI);
		    logger.info("\t\t\tlanguageLabel: " + languageLabel);
		    logger.info("\t\t\tvalueConstraintRemark: " + valueConstraintRemark);
		    logger.info("\t\t\tdefaultURI: " + defaultURI);
		    logger.info("\t\t\tdefaultLiteral: " + defaultLiteral);
		    logger.info("\t\t\tdataTypeURI: " + dataTypeURI);
		    
		    ValueConstraint theValueConstraint = new ValueConstraint(valueEditable, language, languageURI, languageLabel, valueConstraintRemark, defaultURI, defaultLiteral, dataTypeURI);
		    thePropertyTemplate.setValueConstraint(theValueConstraint);
		    
		    JSONArray valueTemplateRefs = valueConstraint.getJSONArray("valueTemplateRefs");
		    for (int k = 0; k < valueTemplateRefs.length(); k++) {
			String valueTemplateRef = valueTemplateRefs.getString(k);
			logger.info("\t\t\tvalueTemplateRef: " + valueTemplateRef);
			theValueConstraint.addValueTemplateRef(valueTemplateRef);
		    }
		    JSONArray useValuesFrom = valueConstraint.getJSONArray("useValuesFrom");
		    for (int k = 0; k < useValuesFrom.length(); k++) {
			String useValueFrom = useValuesFrom.getString(k);
			logger.info("\t\t\tuseValueFrom: " + useValueFrom);
			theValueConstraint.addUseValeFrom(useValueFrom);
		    }
		}
	    }
	}
    }
    
    static void generateQuery(String id, String subjectURI) {
	int var = 1;
	ResourceTemplate resource = resourceHash.get(id);
	logger.info("selecting reqource: " + resource.getId());
	for (PropertyTemplate property : resource.getPropertyTemplates()) {
	    logger.info("\tproperty: " + property.getLabel());
	    logger.info("\t\ttype : " + property.getType());
	    logger.info("\t\trepeatable : " + property.isRepeatable());
	    logger.info("\t\tURI : " + property.getURI());
	    for (String ref : property.getValueConstraint().getValueTemplateRefs()) {
		logger.info("\t\tvalueTemplaceRef : " + ref);
	    }
	}
	StringBuffer buffer = new StringBuffer();
	buffer.append("CONSTRUCT\n\t{\n");
	buffer.append("\t\t" + subjectURI + " ?p ?o .\n");
	generateTriplePatterns(buffer, resource, var+"", "");
	buffer.append("\t}\nFROM <http://share-vde.org>\nFROM <http://share-vde.org/sharevde/rdfBibframe2/Agent/STANFORD>\nWHERE {\n");
	buffer.append("\t\t{ " + subjectURI + " ?p ?o . }\n");
	generateWhereClauses(subjectURI, buffer, resource, var+"", "");
	buffer.append("}\n");
	
	logger.info("query:\n" + buffer);
    }
    
    static void generateTriplePatterns(StringBuffer buffer, ResourceTemplate parent, String prefix, String callPath) {
	int var = 1;
	if (parent == null || callPath.indexOf(parent.getId()) >= 0)
	    return;
	logger.info("template: " + parent.getId());
	for (PropertyTemplate property : parent.getPropertyTemplates()) {
	    switch(property.getType()) {
	    	case "literal" :
	    	case "lookup" :
//		    buffer.append("\t\t?s" + prefix + "_" + var + " ?p" + prefix + "_" + var + " ?o" + prefix + "_" + var + " .\n");
	    	    break;
	    	case "resource" :
	    	case "target" :
		    buffer.append("\t\t?s" + prefix + "_" +  var + " ?p" + prefix + "_" + var + " ?o" + prefix + "_" + var + " . # " + parent.getId() + " : " + property.getLabel() + "\n");
		    for (String name : property.getValueConstraint().getValueTemplateRefs()) {
			ResourceTemplate child = resourceHash.get(name);
			generateTriplePatterns(buffer, child, prefix+"_"+var, callPath+" | "+parent.getId());
		    }
	    	    break;
		default :
		    logger.error("*** unrecognized property type: " + property.getType());
		    break;
	    }
	    var++;
	}
    }

    static void generateWhereClauses(String subjectURI, StringBuffer buffer, ResourceTemplate parent, String prefix, String callPath) {
	int var = 1;
	if (parent == null || callPath.indexOf(parent.getId()) >= 0)
	    return;
	logger.info("template: " + parent.getId());
	for (PropertyTemplate property : parent.getPropertyTemplates()) {
	    switch(property.getType()) {
	    	case "literal" :
	    	case "lookup" :
	    	case "target" :
//		    buffer.append("\tUNION { # " + property.getType() + "\n");
//		    buffer.append("\t\t" + subjectURI + " <" + property.getURI() + "> ?s" + prefix + "_" + var + " .\n");
//		    buffer.append("\t\t?s" + prefix + "_" + var + " ?p" + prefix + "_" + var + " ?o" + prefix + "_" + var + " .\n");
//		    buffer.append("\t}\n");
	    	    break;
	    	case "resource" :
//		    buffer.append("\t\t?s" + prefix + "_" +  var + " ?p" + prefix + "_" + var + " ?o" + prefix + "_" + var + " . # " + parent.getId() + " : " + property.getLabel() + "\n");
		    buffer.append("\tUNION { # " + property.getType() + "\n");
//		    buffer.append("\t\t" + subjectURI + " <" + property.getURI() + "> ?s" + prefix + "_" + var + " . # " + callPath + "\n");
		    buffer.append("\t\t?s" + prefix + "_" + var + " ?p" + prefix + "_" + var + " ?o" + prefix + "_" + var + " . # " + callPath + " : " + property.getURI() + "\n");
		    buffer.append("\t}\n");
		    for (String name : property.getValueConstraint().getValueTemplateRefs()) {
			ResourceTemplate child = resourceHash.get(name);
			generateWhereClauses(subjectURI, buffer, child, prefix+"_"+var, callPath+" | "+parent.getId());
		    }
	    	    break;
		default :
		    logger.error("*** unrecognized property type: " + property.getType());
		    break;
	    }
	    var++;
	}
    }

    static void simpleStmt(String queryString) {
        try {
            logger.info("executing " + queryString + "...");
            PreparedStatement beginStmt = conn.prepareStatement(queryString);
            beginStmt.executeUpdate();
            beginStmt.close();
        } catch ( Exception e ) {
            logger.error("Error in database initialization: ", e);
        }
    }
}
