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

public class SubjectsIndexer extends ThreadedIndexer implements Runnable {

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("loc_subjects");

	String query =
		"SELECT ?uri ?subject WHERE { "
		+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
    		+ "} ";
	queue(query);
	instantiateWriter();
	process(MethodHandles.lookup().lookupClass());
	closeWriter();
    }

    int threadID = 0;
    
    public SubjectsIndexer(int threadID) {
	logger.info("SubjectsIndexer thread: " + threadID);
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
		indexLoCSubjects(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexLoCSubjects(String URI) throws CorruptIndexException, IOException, InterruptedException {
	    String subject = null;
	String query =
		"SELECT ?subject WHERE { "
		+ "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
    		+ "} ";
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

	 theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	count++;
    }
    
}
