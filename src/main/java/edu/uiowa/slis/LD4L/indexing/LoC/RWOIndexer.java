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

public class RWOIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("loc_rwo");
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
	    case "organizations":
		query = "SELECT ?uri ?name ?rwo WHERE { "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
			+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#CorporateName> . "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#identifiesRWO> ?rwo . "
			+ "} ";
		break;
	    case "titles":
		query = "SELECT ?uri ?name ?rwo WHERE { "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
			+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#Title> . "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#identifiesRWO> ?rwo . "
			+ "} ";
		break;
	    case "persons":
		query = "SELECT ?uri ?name ?rwo WHERE { "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
			+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#PersonalName> . "
			+ "?uri <http://www.loc.gov/mads/rdf/v1#identifiesRWO> ?rwo . "
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
    
    public RWOIndexer(int threadID) {
	logger.info("RWOIndexer thread: " + threadID);
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
	String RWO = null;
	String name = null;
	String query = null;
	switch (subauthority) {
	case "organizations":
	    query = "SELECT ?uri ?name ?rwo WHERE { "
		    + "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		    + "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#CorporateName> . "
		    + "?uri <http://www.loc.gov/mads/rdf/v1#identifiesRWO> ?rwo . " + "} ";
	    break;
	case "titles":
	    query = "SELECT ?uri ?name ?rwo WHERE { "
		    + "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		    + "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#Title> . "
		    + "?uri <http://www.loc.gov/mads/rdf/v1#identifiesRWO> ?rwo . " + "} ";
	    break;
	case "persons":
	    query = "SELECT ?uri ?name ?rwo WHERE { "
		    + "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		    + "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#PersonalName> . "
		    + "?uri <http://www.loc.gov/mads/rdf/v1#identifiesRWO> ?rwo . " + "} ";
	    break;
	}
	logger.trace("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    RWO = sol.get("?rwo").asLiteral().getString();
	    name = sol.get("?name").asLiteral().getString();
	}
	logger.info("[" + threadID + "] \tname: " + name);

	Document theDocument = new Document();
	theDocument.add(new StringField("uri", RWO, Field.Store.YES));
	theDocument.add(new StringField("name", name, Field.Store.YES));
	theDocument.add(new TextField("content", retokenizeString(name, true), Field.Store.NO));
	annotateLoCName(URI, theDocument, "hasVariant", "variantLabel");
//	annotateLoCName(URI, theDocument, "fieldOfActivity", "label");
//	annotateLoCName(URI, theDocument, "fieldOfActivity", "authoritativeLabel");
//	annotateLoCName(URI, theDocument, "occupation", "authoritativeLabel");
	    
	theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	if (++count % 100000 == 0)
	    logger.info("[" + threadID + "] count: " + count);
    }
    
}
