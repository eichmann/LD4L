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

public class RDAIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;
    static String realSubauthority = null;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("rda");
	String[] subauthorities = getSubauthorities();
	logger.info("subauthorities: " + arrayString(subauthorities));

	for (String subauthority : subauthorities) {
	    logger.info("");
	    logger.info("indexing subauthority " + subauthority);
	    logger.info("");
	    setSubauthority(subauthority.toLowerCase());
	    realSubauthority = subauthority;
	    String query = 
		" SELECT DISTINCT ?uri ?subject where { "
		+ "  ?uri <http://www.w3.org/2004/02/skos/core#inScheme> <http://rdaregistry.info/termList/" + subauthority + "> . "
		+ "  ?uri <http://www.w3.org/2004/02/skos/core#prefLabel> ?subject . "
		+ "  FILTER (lang(?subject) = 'en') "
		+ "}";
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
    
    public RDAIndexer(int threadID) {
	logger.info("RDAIndexer thread: " + threadID);
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
		indexRDA(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexRDA(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String name = null;
	String query = 
		" SELECT DISTINCT ?name where { "
		+ "<" + URI + ">  <http://www.w3.org/2004/02/skos/core#inScheme> <http://rdaregistry.info/termList/" + realSubauthority + "> . "
		+ "<" + URI + ">  <http://www.w3.org/2004/02/skos/core#prefLabel> ?name . "
		+ "  FILTER (lang(?name) = 'en') " + "}";
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
		  "SELECT DISTINCT ?lab WHERE { "
		+ "<" + URI + "> <http://www.w3.org/2004/02/skos/core#prefLabel> ?lab . "
		+ "}";
	    ResultSet ars = getResultSet(prefix + query1);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String name2 = asol.get("?lab").asLiteral().getString();
		logger.info("\t\tprefLabel: " + name2);
		theDocument.add(new TextField("content", retokenizeString(name2, true), Field.Store.NO));
	    }

	    String query2 = 
		  "SELECT DISTINCT ?lab WHERE { "
		+ "<" + URI + "> <http://www.w3.org/2004/02/skos/core#altLabel> ?lab . "
		+ "}";
	    ars = getResultSet(prefix + query2);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String altname = asol.get("?lab").asLiteral().getString();
		logger.info("\t\taltLabel: " + altname);
		theDocument.add(new TextField("content", retokenizeString(altname, true), Field.Store.NO));
	    }

	    String query3 = 
		  "SELECT DISTINCT ?def WHERE { "
		+ "<" + URI + "> <http://www.w3.org/2004/02/skos/core#definition> ?def . "
		+ "}";
	    ars = getResultSet(prefix + query3);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String definition = asol.get("?def").asLiteral().getString();
		logger.info("\t\tdefinition: " + definition);
		theDocument.add(new TextField("content", retokenizeString(definition, true), Field.Store.NO));
	    }

	theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	if (++count % 100000 == 0)
	    logger.info("[" + threadID + "] count: " + count);
    }
    
}
