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

public class AgrovocIndexer extends ThreadedIndexer implements Runnable {

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("agrovoc");

	String query =
		"SELECT DISTINCT ?uri ?subject  WHERE { "
		+ "?uri rdf:type skos:Concept . "
		+ "?uri skos:prefLabel ?subject . "
		+ "FILTER (lang(?subject) = 'en') "
    		+ "}";
	queue(query);
	instantiateWriter();
	process(MethodHandles.lookup().lookupClass());
	closeWriter();
    }

    int threadID = 0;
    
    public AgrovocIndexer(int threadID) {
	logger.info("AgrovocIndexer thread: " + threadID);
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
		indexAgrovoc(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexAgrovoc(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String subject = null;
	String query =
		"SELECT DISTINCT ?label  WHERE { "
		+ "<" + URI +"> rdf:type skos:Concept . "
		+ "<" + URI +"> skos:prefLabel ?label . "
		+ "FILTER (lang(?label) = 'en') "
    		+ "}";
	logger.trace("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    subject = sol.get("?label").asLiteral().getString();
	}
	logger.info("\tprefLabel: " + subject);

	Document theDocument = new Document();
	theDocument.add(new StringField("uri", URI, Field.Store.YES));
	theDocument.add(new StringField("name", subject, Field.Store.YES));
	theDocument.add(new TextField("content", retokenizeString(subject, true), Field.Store.NO));
	theDocument.add(new TextField("prefcontent", retokenizeString(subject, true), Field.Store.NO));

	    String query1 = 
		  "SELECT DISTINCT ?preflabel WHERE { "
			  + "<" + URI + "> skos:prefLabel ?preflabel . "
		+ "}";
	    ResultSet prs = getResultSet(prefix + query1);
	    while (prs.hasNext()) {
		QuerySolution psol = prs.nextSolution();
		String preflabel = psol.get("?preflabel").asLiteral().getString();
		logger.info("\tpref label: " + preflabel);
		theDocument.add(new TextField("content", retokenizeString(preflabel, true), Field.Store.NO));
		theDocument.add(new TextField("prefcontent", retokenizeString(preflabel, true), Field.Store.NO));
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
	count++;
    }
    
}
