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

public class MeSHIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;
    static String realSubauthority = null;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("mesh");
	String[] subauthorities = getSubauthorities();
	logger.info("subauthorities: " + arrayString(subauthorities));

	for (String subauthority : subauthorities) {
	    logger.info("");
	    logger.info("indexing subauthority " + subauthority);
	    logger.info("");
	    setSubauthority(subauthority.toLowerCase());
	    realSubauthority = subauthority;
	    String query = 
			" SELECT DISTINCT ?uri ?subject where { "+
				"  ?uri rdf:type <http://id.nlm.nih.gov/mesh/vocab#" + subauthority + "> . "+
				"  ?uri <http://www.w3.org/2000/01/rdf-schema#label> ?subject . "+
				"  ?uri <http://id.nlm.nih.gov/mesh/vocab#active> true . "+
				"}";
	    queue(query);
	    instantiateWriter();
	    process(MethodHandles.lookup().lookupClass());
	    closeWriter();
	}
	logger.info("");
	logger.info("merging subauthorities...");
	logger.info("");
	mergeSubauthorities();
    }

    int threadID = 0;
    
    public MeSHIndexer(int threadID) {
	logger.info("MeSHIndexer thread: " + threadID);
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
		indexMeSH(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexMeSH(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String name = null;
	String query = 
		" SELECT DISTINCT ?name where { "+
			"<" + URI + "> rdf:type <http://id.nlm.nih.gov/mesh/vocab#" + realSubauthority + "> . "+
			"<" + URI + "> <http://www.w3.org/2000/01/rdf-schema#label> ?name . "+
			"<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#active> true . "+
			"}";
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
	theDocument.add(new TextField("content", retokenizeString(name, true), Field.Store.NO));

	    String query1 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#concept> ?c . "
			  + "?c <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ResultSet ars = getResultSet(prefix + query1);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\tconcept: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }
	    
	    query1 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#preferredConcept> ?c . "
			  + "?c <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ars = getResultSet(prefix + query1);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\tpreferred concept: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }
	    
	    String query2 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#concept> ?c . "
			  + "?bc <http://id.nlm.nih.gov/mesh/vocab#broaderConcept> ?c . "
			  + "?bc <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ars = getResultSet(prefix + query2);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\tnarrower concept: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }
	    
	    query2 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#preferredConcept> ?c . "
			  + "?bc <http://id.nlm.nih.gov/mesh/vocab#broaderConcept> ?c . "
			  + "?bc <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ars = getResultSet(prefix + query2);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\tpreferred narrower concept: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }
	    
	    String query3 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#concept> ?c . "
			  + "?nc <http://id.nlm.nih.gov/mesh/vocab#narrowerConcept> ?c . "
			  + "?nc <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ars = getResultSet(prefix + query3);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\tbroader concept: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }
	    
	    query3 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#preferredConcept> ?c . "
			  + "?nc <http://id.nlm.nih.gov/mesh/vocab#narrowerConcept> ?c . "
			  + "?nc <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ars = getResultSet(prefix + query3);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\tpreferred broader concept: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }
	    
	    String query4 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#concept> ?c . "
			  + "?rc <http://id.nlm.nih.gov/mesh/vocab#relatedConcept> ?c . "
			  + "?rc <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ars = getResultSet(prefix + query4);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\trelated concept: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }
	    
	    query4 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#preferredConcept> ?c . "
			  + "?rc <http://id.nlm.nih.gov/mesh/vocab#relatedConcept> ?c . "
			  + "?rc <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ars = getResultSet(prefix + query4);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\tpreferred related concept: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }
	    
	    String query5 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "<" + URI + "> <http://id.nlm.nih.gov/mesh/vocab#useInstead> ?c . "
			  + "?c <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ars = getResultSet(prefix + query5);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\tuse instead: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }
	    
	    String query6 = 
		  "SELECT DISTINCT ?concept WHERE { "
			  + "?c <http://id.nlm.nih.gov/mesh/vocab#mappedTo> <" + URI + "> . "
			  + "?c <http://www.w3.org/2000/01/rdf-schema#label> ?concept . "
		+ "}";
	    ars = getResultSet(prefix + query6);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String concept = asol.get("?concept").asLiteral().getString();
		logger.info("\tmapped to: " + concept);
		theDocument.add(new TextField("content", retokenizeString(concept, true), Field.Store.NO));
	    }

	theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	if (++count % 100000 == 0)
	    logger.info("[" + threadID + "] count: " + count);
    }
    
}
