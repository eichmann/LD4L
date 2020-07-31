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

public class DemographicsIndexer extends ThreadedIndexer implements Runnable {

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("loc_demographics");
	prefix =    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
		    + " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
		    + " PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
		    + " PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
		    + " PREFIX owl: <http://www.w3.org/2002/07/owl#> "
		    + " PREFIX umls: <http://bioportal.bioontology.org/ontologies/umls/> "
		    + " PREFIX getty: <http://vocab.getty.edu/ontology#> "
		    + " PREFIX schema: <http://schema.org/> "
		    + " PREFIX mads: <http://www.loc.gov/mads/rdf/v1#> "
		    + " PREFIX bib: <http://bib.ld4l.org/ontology/> ";
	String query =
		"SELECT ?uri ?subject WHERE { "
		+ "?uri skos:prefLabel ?subject . "
    		+ "} ";
	queue(query);
	instantiateWriter();
	process(MethodHandles.lookup().lookupClass());
	closeWriter();
    }

    int threadID = 0;
    
    public DemographicsIndexer(int threadID) {
	logger.info("DemographicsIndexer thread: " + threadID);
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
		indexLoCDemographics(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexLoCDemographics(String URI) throws CorruptIndexException, IOException, InterruptedException {
	    String subject = null;
	String query =
		"SELECT ?subject WHERE { "
		+ "<" + URI + "> skos:prefLabel ?subject . "
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
	for (int i = 0; i < 10; i++) // result weighting hack
	    theDocument.add(new TextField("content", retokenizeString(subject, true), Field.Store.NO));

	addWeightedField(theDocument,
		"SELECT DISTINCT ?altlabel WHERE { " + "<" + URI + "> skos:altLabel ?altlabel . " + "}",
		"altlabel", 2);
	addWeightedField(theDocument,
		"SELECT DISTINCT ?broader WHERE { " + "<" + URI
			+ "> <http://www.w3.org/2004/02/skos/core#broader> ?broadURI . "
			+ "?broadURI skos:prefLabel ?broader . " + "}",
		"broader", 1);
	addWeightedField(theDocument,
		"SELECT DISTINCT ?narrower WHERE { " + "<" + URI
			+ "> <http://www.w3.org/2004/02/skos/core#narrower> ?narrowURI . "
			+ "?narrowURI skos:prefLabel ?narrower . " + "}",
		"narrower", 1);

//	 theDocument.add(new StoredField("payload", generatePayload("http://services.ld4l.org/ld4l_services/loc_demographics_lookup.jsp?uri=" + URI)));

	theWriter.addDocument(theDocument);
	count++;
    }
    
}
