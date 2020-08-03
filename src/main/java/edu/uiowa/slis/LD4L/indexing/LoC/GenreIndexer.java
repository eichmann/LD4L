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

public class GenreIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("loc_genre");
	String[] subauthorities = getSubauthorities();
	logger.info("subauthorities: " + arrayString(subauthorities));
	prefix =    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
		    + " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
		    + " PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
		    + " PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
		    + " PREFIX owl: <http://www.w3.org/2002/07/owl#> "
		    + " PREFIX umls: <http://bioportal.bioontology.org/ontologies/umls/> "
		    + " PREFIX getty: <http://vocab.getty.edu/ontology#> "
		    + " PREFIX schema: <http://schema.org/> "
		    + " PREFIX mads: <http://www.loc.gov/mads/rdf/v1#> "
		    + " PREFIX loc: <http://id.loc.gov/vocabulary/identifiers/> "
		    + " PREFIX bib: <http://bib.ld4l.org/ontology/> ";
	for (String subauthority : subauthorities) {
	    logger.info("");
	    logger.info("indexing subauthority " + subauthority);
	    logger.info("");
	    setSubauthority(subauthority);
	    String query = null;
	    switch (subauthority) {
	    case "active":
		query = "SELECT ?uri ?subject WHERE { "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
//		    	+ "FILTER NOT EXISTS {"
//		    	+ "	  ?uri <http://www.loc.gov/mads/rdf/v1#adminMetadata> ?stat . "
//		    	+ "   ?stat <http://id.loc.gov/ontologies/RecordInfo#recordStatus> 'deprecated' . "
//		    	+ "   }"
		    	+ "} ";
		break;
	    case "deprecated":
		 query = "SELECT ?uri ?subject WHERE { "
			    + "?uri <http://www.loc.gov/mads/rdf/v1#variantLabel> ?subject . "
			    + "?uri <http://www.loc.gov/mads/rdf/v1#adminMetadata> ?stat . "
			    + "?stat <http://id.loc.gov/ontologies/RecordInfo#recordStatus> \"deprecated\" . "
			    + "} ";
		break;
	    }
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
    
    public GenreIndexer(int threadID) {
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
		indexLoCGenre(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexLoCGenre(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String subject = null;
	String query = deprecated ?
		    "SELECT ?subject WHERE { "
		    + "?uri <http://www.loc.gov/mads/rdf/v1#variantLabel> ?subject . "
		    + "} "
		:
		    "SELECT ?subject WHERE { "
		    + "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
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

	String query1 = "SELECT DISTINCT ?altlabel WHERE { " + "<" + URI + "> skos:altLabel ?altlabel . " + "}";
	ResultSet prs = getResultSet(prefix + query1);
	while (prs.hasNext()) {
	    QuerySolution psol = prs.nextSolution();
	    String altlabel = psol.get("?altlabel").asLiteral().getString();
	    logger.info("\talt label: " + altlabel);
	    theDocument.add(new TextField("content", altlabel, Field.Store.NO));
	}

	theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	count++;
    }
    
}
