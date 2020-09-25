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

public class InstancesIndexer extends ThreadedIndexer implements Runnable {

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("loc_instances");

	String query =
		"SELECT ?uri ?subject WHERE { "
		+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.loc.gov/ontologies/bibframe/Instance> . "
		+ "?uri <http://www.w3.org/2000/01/rdf-schema#label> ?subject . "
    		+ "} ";
	queue(query);
	instantiateWriter();
	process(MethodHandles.lookup().lookupClass());
	closeWriter();
    }

    int threadID = 0;
    
    public InstancesIndexer(int threadID) {
	logger.info("WorksIndexer thread: " + threadID);
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
	    String label = null;
	String query =
		"SELECT ?label WHERE { "
		+ "<" + URI + "> <http://www.w3.org/2000/01/rdf-schema#label> ?label . "
    		+ "} ";
	logger.trace("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    label = sol.get("?label").asLiteral().getString();
	}
	logger.info("\tlabel: " + label);

	    Document theDocument = new Document();
	    theDocument.add(new StringField("uri", URI, Field.Store.YES));
	    theDocument.add(new StringField("name", label, Field.Store.YES));
	    theDocument.add(new TextField("content", retokenizeString(label, true), Field.Store.NO));

	 theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	count++;
    }
    
}
