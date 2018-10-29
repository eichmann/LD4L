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
import java.util.Vector;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import edu.uiowa.slis.LD4L.ShortStory.Author;
import edu.uiowa.slis.LD4L.ShortStory.CommaByExtractor;
import edu.uiowa.slis.LD4L.ShortStory.CommaExtractor;
import edu.uiowa.slis.LD4L.ShortStory.Extractor;
import edu.uiowa.slis.LD4L.ShortStory.ShortStory;
import edu.uiowa.slis.LD4L.ShortStory.SlashExtractor;
import edu.uiowa.slis.LD4L.Sinopia.Profile;
import edu.uiowa.slis.LD4L.Sinopia.PropertyTemplate;
import edu.uiowa.slis.LD4L.Sinopia.ResourceTemplate;
import edu.uiowa.slis.LD4L.Sinopia.ValueConstraint;

public class ShortStoryLoader {
    static Logger logger = Logger.getLogger(ShortStoryLoader.class);
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
	
	Extractor slashExtractor = new SlashExtractor();
	Extractor commaExtractor = new CommaExtractor();
	Extractor commaByExtractor = new CommaByExtractor();
	
	PreparedStatement stmt = conn.prepareStatement("select id,contents from contents.asimov order by id");
	ResultSet rs = stmt.executeQuery();
	while (rs.next()) {
	    int id = rs.getInt(1);
	    String contents = rs.getString(2);
	    Vector<ShortStory> stories = new Vector<ShortStory>();
	    logger.info("id: " + id + "\tcontents: " + contents);
	    
	    String[] entries = contents.split("\\.? *-- *");
	    for (String entry : entries) {
		logger.info("entry: " + entry);
	    }
	    
	    int slashCount = slashExtractor.matchCount(entries);
	    logger.info("\tslash: " + slashCount);
	    if (entries.length - slashCount < 3) {
		for (String entry : entries) {
		    ShortStory story = slashExtractor.extract(entry);
		    logger.info("\t\tstory: " + story);
		    stories.add(story);
		}
	    }
	    
	    int commaCount = commaExtractor.matchCount(entries);
	    logger.info("\tcomma: " + commaCount);
	    if (entries.length - commaCount < 3) {
		for (String entry : entries) {
		    ShortStory story = commaExtractor.extract(entry);
		    stories.add(story);
		    logger.info("\t\tstory: " + story);
		}
	    }
	    
	    int commaByCount = commaByExtractor.matchCount(entries);
	    logger.info("\tcommaBy: " + commaByCount);
	    if (commaCount == 0 && entries.length - commaByCount < 3) {
		for (String entry : entries) {
		    ShortStory story = commaByExtractor.extract(entry);
		    stories.add(story);
		    logger.info("\t\tstory: " + story);
		}
	    }
	    
	    storeStories(id, stories);
	    logger.info("");
	}

    }
    
    static void storeStories(int id, Vector<ShortStory> stories) throws SQLException {
	for (int i = 0; i < stories.size(); i++) {
	    PreparedStatement stmt = conn.prepareStatement("insert into contents.asimov_short values(?,?,?)");
	    stmt.setInt(1, id);
	    stmt.setInt(2, i+1);
	    stmt.setString(3, stories.get(i).getTitle());
	    stmt.execute();
	    stmt.close();
	    
	    for (int j = 0; j < stories.get(i).getAuthors().size(); j++) {
		Author author = stories.get(i).getAuthors().elementAt(j);
		PreparedStatement astmt = conn.prepareStatement("insert into contents.asimov_author values(?,?,?,?,?,?,?,?)");
		astmt.setInt(1, id);
		astmt.setInt(2, i+1);
		astmt.setInt(3, j+1);
		astmt.setString(4, author.getFirstName());
		astmt.setString(5, author.getMiddleName());
		astmt.setString(6, author.getSurname());
		astmt.setString(7, author.getTitle());
		astmt.setString(8, author.getAppendix());
		astmt.execute();
		astmt.close();
	    }
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
