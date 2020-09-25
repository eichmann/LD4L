package edu.uiowa.slis.LD4L.indexing.CERL;

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

public class CorporateIndexer extends ThreadedIndexer implements Runnable {

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("cerl_corporate");

	String query =
		" SELECT DISTINCT ?uri ?subject where { "+
		"  ?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdvocab.info/uri/schema/FRBRentitiesRDA/CorporateBody> . "+
		"  ?uri <http://rdvocab.info/ElementsGr2/nameOfTheCorporateBody> ?subject . "+
		"}";
	queue(query);
	instantiateWriter();
	process(MethodHandles.lookup().lookupClass());
	closeWriter();
    }

    int threadID = 0;
    
    public CorporateIndexer(int threadID) {
	logger.info("CorporateIndexer thread: " + threadID);
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
		indexLoCPerformance(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexLoCPerformance(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String subject = null;
	String query =
        	" SELECT DISTINCT ?uri ?subject where { "+
        	"<" + URI +"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdvocab.info/uri/schema/FRBRentitiesRDA/CorporateBody> . "+
        	"<" + URI +"> <http://rdvocab.info/ElementsGr2/nameOfTheCorporateBody> ?subject . "+
        	"}";
	logger.trace("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    subject = sol.get("?subject").asLiteral().getString();
	}
	logger.info("\tprefLabel: " + subject);

	Document theDocument = new Document();
	theDocument.add(new StringField("uri", URI, Field.Store.YES));
	theDocument.add(new StringField("name", subject, Field.Store.YES));
	theDocument.add(new TextField("content", retokenizeString(subject, true), Field.Store.NO));

	    String query2 = 
		  "SELECT DISTINCT ?altlabel WHERE { "
			  + "<" + URI + "> <http://rdvocab.info/ElementsGr2/variantNameForTheCorporateBody> ?altlabel . "
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
