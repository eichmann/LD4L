package edu.uiowa.slis.LD4L.indexing.LoC;

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

public class NamesIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("loc_names");
	String[] subauthorities = getSubauthorities();
	logger.info("subauthorities: " + arrayString(subauthorities));

	for (String subauthority : subauthorities) {
	    if (args.length > 0 && !args[0].equals(subauthority))
		continue;
	    logger.info("");
	    logger.info("indexing subauthority " + subauthority);
	    logger.info("");
	    setSubauthority(subauthority);
	    String query = null;
	    switch (subauthority) {
	    case "organizations":
		query = "SELECT ?uri ?subject WHERE { "
				+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
				+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#CorporateName> . "
    			+ "} ";
		break;
	    case "titles":
		query = "SELECT ?uri ?subject WHERE { "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
			+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#Title> . "
			+ "} ";
		break;
	    case "persons":
		query = "SELECT ?uri ?subject WHERE { "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
			+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#PersonalName> . "
			+ "} ";
		break;
	    case "family":
		query = "SELECT ?uri ?subject WHERE { "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
			+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#FamilyName> . "
			+ "} ";
		break;
	    case "geographic":
		query = "SELECT ?uri ?subject WHERE { "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
			+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#Geographic> . "
			+ "} ";
		break;
	    case "conference":
		query = "SELECT ?uri ?subject WHERE { "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
			+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#ConferenceName> . "
			+ "} ";
		break;
	    }
	    queue(query);
	    instantiateWriter();
	    process(MethodHandles.lookup().lookupClass());
	    closeWriter();
	}
	if (args.length > 0 && args[0].equals("-merge")) {
	    logger.info("");
	    logger.info("merging subauthorities...");
	    logger.info("");
	    mergeSubauthorities();
	}
    }

    int threadID = 0;
    
    public NamesIndexer(int threadID) {
	logger.info("PersonsIndexer thread: " + threadID);
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
		indexLoCNames(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexLoCNames(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String name = null;
	String query = null;
	switch (subauthority) {
	case "organizations":
	    query = "SELECT ?name WHERE { "
		    + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		    + "<" + URI + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#CorporateName> . " + "} ";
	    break;
	case "titles":
	    query = "SELECT ?name WHERE { "	
		    + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		    + "<" + URI + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#Title> . " + "} ";
	    break;
	case "persons":
	    query = "SELECT ?name WHERE { "
		    + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		    + "<" + URI + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#PersonalName> . " + "} ";
	    break;
	case "family":
	    query = "SELECT ?name WHERE { "
		    + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		    + "<" + URI + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#FamilyName> . "
		    + "} ";
	    break;
	case "geographic":
	    query = "SELECT ?name WHERE { "
		    + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		    + "<" + URI + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#Geographic> . "
		    + "} ";
	    break;
	case "conference":
	    query = "SELECT ?name WHERE { "
		    + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		    + "<" + URI + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#ConferenceName> . "
		    + "} ";
	    break;
	}
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
	annotateLoCName(URI, theDocument, "hasVariant", "variantLabel");
	annotateLoCName(URI, theDocument, "fieldOfActivity", "label");
	annotateLoCName(URI, theDocument, "fieldOfActivity", "authoritativeLabel");
	annotateLoCName(URI, theDocument, "occupation", "authoritativeLabel");

	theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	if (++count % 100000 == 0)
	    logger.info("[" + threadID + "] count: " + count);
    }
    
}
