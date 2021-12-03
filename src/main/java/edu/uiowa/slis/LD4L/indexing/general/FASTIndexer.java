package edu.uiowa.slis.LD4L.indexing.general;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;

import edu.uiowa.slis.LD4L.indexing.ThreadedIndexer;

public class FASTIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("fast");
	String[] subauthorities = getSubauthorities();
	logger.info("subauthorities: " + arrayString(subauthorities));
	// CreativeWork|Organization|GeoCoordinates|Place|Person|facet-Topical|facet-FormGenre|Event
	
	for (String subauthority : subauthorities) {
	    if (args.length > 0 && !args[0].equals(subauthority))
	    	continue;
	    logger.info("");
	    logger.info("indexing subauthority " + subauthority);
	    logger.info("");
	    setSubauthority(subauthority);
	    String query = null;
	    if (subauthority.contains("facet"))
		query =
			"SELECT DISTINCT ?uri ?subject WHERE { "
				+ "?uri rdf:type schema:Intangible . "
				+ "?uri skos:inScheme <http://id.worldcat.org/fast/ontology/1.0/#" + subauthority + "> . "
				+ "?uri skos:prefLabel ?subject . "
		    		+ "}";
	    else if (subauthority.equals("Meeting"))
			query =
			"SELECT DISTINCT ?uri ?subject WHERE { "
				+ "?uri rdf:type bf:" + subauthority + " . "
				+ "?uri <http://schema.org/name> ?subject . "
		    		+ "}";
	    else if (subauthority.equals("Periodization"))
			query =
			"SELECT DISTINCT ?uri ?subject WHERE { "
				+ "?uri rdf:type prod:" + subauthority + " . "
				+ "?uri <http://schema.org/name> ?subject . "
		    		+ "}";
	    else
		query =
			"SELECT DISTINCT ?uri ?subject WHERE { "
				+ "?uri rdf:type <http://schema.org/" + subauthority + "> . "
				+ "?uri <http://schema.org/name> ?subject . "
		    		+ "}";
	    queue(query);
	    instantiateWriter();
	    process(MethodHandles.lookup().lookupClass());
	    closeWriter();
	}
	
	if (args.length == 0 || (args.length > 0 && args[0].equals("-merge"))) {
		// create the full merged index
		logger.info("");
		logger.info("merging subauthorities...");
		logger.info("");
		mergeSubauthorities();

		// create the event/meeting-specific index
		logger.info("");
		logger.info("merging Entity and Meeting subauthorities...");
		logger.info("");
		resetSubauthorities("Event|Meeting");
		setAltLucenePath(lucenePath + "_event_meeting");
		mergeSubauthorities();
	}
    }
    
    int threadID = 0;
    
    public FASTIndexer(int threadID) {
	logger.info("FASTIndexer thread: " + threadID);
	this.threadID = threadID;
    }

    @Override
    public void run() {
	while (!uriQueue.isCompleted()) {
	    String uri = uriQueue.dequeue();
	    if (uri == null)
		return;
	    logger.info("[" + threadID + "] indexing: " + uri);
	    try {
		indexGetty(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexGetty(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String name = null;
	    String query = null;
	    if (subauthority.contains("facet"))
		query =
			"SELECT DISTINCT ?name WHERE { "
				+ "<" + URI + "> rdf:type schema:Intangible . "
				+ "<" + URI + "> skos:inScheme <http://id.worldcat.org/fast/ontology/1.0/#" + subauthority + "> . "
				+ "<" + URI + "> skos:prefLabel ?name . "
		    		+ "}";
	    else if (subauthority.equals("Meeting"))
			query =
			"SELECT DISTINCT ?name WHERE { "
				+ "<" + URI + "> rdf:type bf:" + subauthority + " . "
				+ "<" + URI + "> <http://schema.org/name> ?name . "
		    		+ "}";
	    else if (subauthority.equals("Periodization"))
			query =
			"SELECT DISTINCT ?name WHERE { "
				+ "<" + URI + "> rdf:type prod:" + subauthority + " . "
				+ "<" + URI + "> <http://schema.org/name> ?name . "
		    		+ "}";
	    else
		query =
			"SELECT DISTINCT ?name WHERE { "
				+ "<" + URI + "> rdf:type <http://schema.org/" + subauthority + "> . "
				+ "<" + URI + "> <http://schema.org/name> ?name . "
		    		+ "}";
	logger.trace("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    name = sol.get("?name").asLiteral().getString();
	}
	logger.info("[" + threadID + "] \tname: " + name);

	Document theDocument = new Document();
	theDocument.add(new StringField("uri", URI, Field.Store.YES));
	theDocument.add(new StringField("name", name, Field.Store.YES));
	theDocument.add(new StringField("name_lower", name.toLowerCase(), Field.Store.YES));
	theDocument.add(new TextField("content", retokenizeString(name, true), Field.Store.NO));

	    String query1 = 
		  "SELECT DISTINCT ?name WHERE { "
			  + "<" + URI + "> <http://schema.org/name> ?name . "
		+ "}";
	    ResultSet prs = getResultSet(prefix + query1);
	    while (prs.hasNext()) {
		QuerySolution psol = prs.nextSolution();
		String name2 = psol.get("?name").asLiteral().getString();
		logger.info("\tname: " + name2);
		theDocument.add(new TextField("content", retokenizeString(name2, true), Field.Store.NO));
	    }
	    
	    String query2 = 
		  "SELECT DISTINCT ?altlabel WHERE { "
			  + "<" + URI + "> skos:altLabel ?altlabel . "
		+ "}";
	    ResultSet ars = getResultSet(prefix + query2);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String altlabel = asol.get("?altlabel").asLiteral().getString();
		logger.info("\talt label: " + altlabel);
		theDocument.add(new TextField("content", retokenizeString(altlabel, true), Field.Store.NO));
	    }

	theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	if (++count % 100000 == 0)
	    logger.info("[" + threadID + "] count: " + count);
    }
    
}
