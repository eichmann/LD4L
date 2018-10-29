package edu.uiowa.slis.LD4L;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.Syntax;
import org.apache.jena.tdb.TDBFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.uiowa.PubMedCentral.entity.Person;
import edu.uiowa.slis.LD4L.ShortStory.Author;
import edu.uiowa.slis.LD4L.ShortStory.CommaByExtractor;
import edu.uiowa.slis.LD4L.ShortStory.CommaExtractor;
import edu.uiowa.slis.LD4L.ShortStory.Extractor;
import edu.uiowa.slis.LD4L.ShortStory.ShortStory;
import edu.uiowa.slis.LD4L.ShortStory.SlashExtractor;
import edu.uiowa.slis.LD4L.Sinopia.Profile;
import edu.uiowa.slis.LD4L.Sinopia.ResourceTemplate;

public class ShortStoryLoader {
    static Logger logger = Logger.getLogger(ShortStoryLoader.class);
    static final String networkHostName = "localhost";
    static Connection conn = null;
    static String thePath = "/Users/eichmann/downloads/sinopia_sample_profiles-master/verso";
    static Hashtable<String, Profile> profileHash = new Hashtable<String, Profile>();
    static Hashtable<String, ResourceTemplate> resourceHash = new Hashtable<String, ResourceTemplate>();
    
    static String group = "asimov";
//    static String group = "contents";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
	PropertyConfigurator.configure(args[0]);
	logger.info("connecting to database...");
	Person.initialize();
	Class.forName("org.postgresql.Driver");
	Properties props = new Properties();
	props.setProperty("user", "eichmann");
	props.setProperty("password", "translational");
	// props.setProperty("sslfactory",
	// "org.postgresql.ssl.NonValidatingFactory");
	// props.setProperty("ssl", "true");
	conn = DriverManager.getConnection("jdbc:postgresql://" + networkHostName + "/ld4l", props);
	
//	populateContents();
	
	simpleStmt("truncate contents."+group+"_short");
	simpleStmt("truncate contents."+group+"_author");
	
	Extractor slashExtractor = new SlashExtractor();
	Extractor commaExtractor = new CommaExtractor();
	Extractor commaByExtractor = new CommaByExtractor();
	
	PreparedStatement stmt = conn.prepareStatement("select id,title,contents from contents."+group+" order by id");
	ResultSet rs = stmt.executeQuery();
	while (rs.next()) {
	    int id = rs.getInt(1);
	    String title = rs.getString(2);
	    String contents = rs.getString(3);
	    if (contents.endsWith("."))
		contents = contents.substring(0, contents.length()-1);
	    Vector<ShortStory> stories = new Vector<ShortStory>();
	    logger.info("id: " + id + "\ttitle: " + title);
	    logger.info("\tcontents: " + contents);
	    
	    String[] entries = contents.split("\\.? *-- *"); // need to do alternative splits = e.g. [a-z]. and give back the letter
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
	    PreparedStatement stmt = conn.prepareStatement("insert into contents."+group+"_short values(?,?,?)");
	    stmt.setInt(1, id);
	    stmt.setInt(2, i+1);
	    stmt.setString(3, stories.get(i).getTitle());
	    stmt.execute();
	    stmt.close();
	    
	    for (int j = 0; j < stories.get(i).getAuthors().size(); j++) {
		Author author = stories.get(i).getAuthors().elementAt(j);
		PreparedStatement astmt = conn.prepareStatement("insert into contents."+group+"_author values(?,?,?,?,?,?,?,?)");
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
    
    /*
     * <http://id.loc.gov/resources/works/c000306502> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.loc.gov/ontologies/bibframe/Text> .
     * <http://id.loc.gov/resources/works/c000306502> <http://www.w3.org/2000/01/rdf-schema#label> "Astounding; John W. Campbell memorial anthology." .
     * <http://id.loc.gov/resources/instances/c0003065020001> <http://id.loc.gov/ontologies/bibframe/instanceOf> <http://id.loc.gov/resources/works/c000306502> .
     * <http://id.loc.gov/resources/instances/c0003065020001> <http://www.w3.org/2000/01/rdf-schema#label> "Astounding; John W. Campbell memorial anthology." .
     * <http://id.loc.gov/resources/instances/c0003065020001> <http://id.loc.gov/ontologies/bibframe/tableOfContents> _:bnode866456721792019327 .
     * _:bnode866456721792019327 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.loc.gov/ontologies/bibframe/TableOfContents> .
     * _:bnode866456721792019327 <http://www.w3.org/2000/01/rdf-schema#label> "Asimov, I. Introduction: The father of science fiction.--Anderson, P. Lodestar.--Asimov, I. Thiotimoline to the stars.--Bester, A. Something up there likes me.--Clement, H. Lecture demonstration.--Cogswell, T. R. and Thomas, T. L. Early bird.--De Camp, L. S. The Emperor's fan.--Dickson, G. R. Brothers.--Harrison, H. The mothballed spaceship.--Reynolds, M. Black sheep astray.--Simak, C. D. Epilog.--Smith, G. O. Interlude.--Sturgeon, T. Helix the cat.--Cogswell, T. R. Probability zero! The population implosion.--Afterword." .

     */

    static void populateContents() throws SQLException {
	String queryString =
		"SELECT ?work ?title ?contents WHERE { "
		+ "?instance <http://id.loc.gov/ontologies/bibframe/tableOfContents> ?x ."
		+ "?x <http://www.w3.org/2000/01/rdf-schema#label> ?contents . "
		+ "?instance <http://www.w3.org/2000/01/rdf-schema#label> ?title . "
		+ "?instance <http://id.loc.gov/ontologies/bibframe/instanceOf> ?work . "
		+ "?work <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.loc.gov/ontologies/bibframe/Text> . "
    		+ "}";
	Dataset dataset = TDBFactory.createDataset("/Volumes/Pegasus3/Corpora/LoC_id/testing");
	Query query = QueryFactory.create(queryString, Syntax.syntaxARQ);
	QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
	org.apache.jena.query.ResultSet rs = qexec.execSelect();
	PreparedStatement stmt = conn.prepareStatement("insert into contents.contents(work,title,contents) values(?,?,?)");
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String work = sol.get("?work").toString();
	    String title = sol.get("?title").asLiteral().getString();
	    String contents = sol.get("?contents").asLiteral().getString();
	    logger.info("\twork: " + work + "\ttitle: " + title);
	    logger.info("\t\tcontents: " + contents);

	    stmt.setString(1, work);
	    stmt.setString(2, title);
	    stmt.setString(3, contents);
	    stmt.execute();
	}
	stmt.close();
    }
}
