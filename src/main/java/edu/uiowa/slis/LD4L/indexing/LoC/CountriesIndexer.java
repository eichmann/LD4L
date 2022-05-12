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

public class CountriesIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("loc_countries");

	    String query = "SELECT ?uri ?subject WHERE { "
			+ "?uri rdf:type skos:Concept . "
			+ "?uri skos:prefLabel ?subject . "
		    + "} ";
	    queue(query);
	    instantiateWriter();
	    process(MethodHandles.lookup().lookupClass());
	    closeWriter();
    }

    int threadID = 0;
    
    public CountriesIndexer(int threadID) {
	logger.info("CulturalOrganizationsIndexer thread: " + threadID);
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
	    	indexLoCCulturalOrganization(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexLoCCulturalOrganization(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String name = null;
	String query = "SELECT ?subject WHERE { "
		    + "<" + URI + "> skos:prefLabel ?subject . "
		    + "} ";
	logger.trace("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    name = sol.get("?subject").asLiteral().getString();
	}
	logger.info("\tprefLabel: " + name);

	Document theDocument = new Document();
	theDocument.add(new StringField("uri", URI, Field.Store.YES));
	theDocument.add(new StringField("name", name, Field.Store.YES));
	theDocument.add(new StringField("name_lower", name.toLowerCase(), Field.Store.YES));
	theDocument.add(new TextField("prefcontent", retokenizeString(name, true), Field.Store.NO));
	theDocument.add(new TextField("content", retokenizeString(name, true), Field.Store.NO));

	String query1 = "SELECT ?notation WHERE { " + "<" + URI + "> skos:notation ?notation . " + "}";
	ResultSet prs = getResultSet(prefix + query1);
	while (prs.hasNext()) {
	    QuerySolution psol = prs.nextSolution();
	    String notation = psol.get("?notation").asLiteral().getString();
	    logger.info("\tnotation: " + notation);
	    theDocument.add(new TextField("content", notation, Field.Store.NO));
	}

	query1 = "SELECT ?label WHERE { " + "<" + URI + "> skos:broader ?b . ?b skos:prefLabel ?label . " + "}";
	prs = getResultSet(prefix + query1);
	while (prs.hasNext()) {
	    QuerySolution psol = prs.nextSolution();
	    String label = psol.get("?label").asLiteral().getString();
	    logger.info("\tbroader label: " + label);
	    theDocument.add(new TextField("content", label, Field.Store.NO));
	}

	query1 = "SELECT ?label WHERE { " + "<" + URI + "> skos:narrower ?n . ?n skos:prefLabel ?label . " + "}";
	prs = getResultSet(prefix + query1);
	while (prs.hasNext()) {
	    QuerySolution psol = prs.nextSolution();
	    String label = psol.get("?label").asLiteral().getString();
	    logger.info("\tnarrower label: " + label);
	    theDocument.add(new TextField("content", label, Field.Store.NO));
	}

	query1 = "SELECT ?label WHERE { " + "<" + URI + "> skos:altLabel ?label . " + "}";
	prs = getResultSet(prefix + query1);
	while (prs.hasNext()) {
	    QuerySolution psol = prs.nextSolution();
	    String label = psol.get("?label").asLiteral().getString();
	    logger.info("\taltlabel: " + label);
	    theDocument.add(new TextField("content", label, Field.Store.NO));
	}

	query1 = "SELECT ?label WHERE { " + "<" + URI + "> skos:related ?r . ?r skos:prefLabel ?label . " + "}";
	prs = getResultSet(prefix + query1);
	while (prs.hasNext()) {
	    QuerySolution psol = prs.nextSolution();
	    String label = psol.get("?label").asLiteral().getString();
	    logger.info("\trelated: " + label);
	    theDocument.add(new TextField("content", label, Field.Store.NO));
	}

	theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	count++;
    }
    
}
