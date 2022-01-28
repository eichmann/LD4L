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

public class ISNIIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;
    static boolean strictMode = false;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("isni");
	String[] subauthorities = getSubauthorities();
	logger.info("subauthorities: " + arrayString(subauthorities));

	for (String subauthority : subauthorities) {
	    logger.info("");
	    logger.info("indexing subauthority " + subauthority);
	    logger.info("");
	    setSubauthority(subauthority);
	    String query = 
			" SELECT ?uri ?subject where { " +
				"  ?uri rdf:type <http://schema.org/" + subauthority + "> . " +
				"  ?uri schema:alternateName ?subject . "
				+ " FILTER NOT EXISTS {"
				+ "    ?uri schema:alternateName ?subject2"
				+ "    FILTER (STRLEN(STR(?subject2)) > STRLEN(STR(?subject)) && STR(?subject2) > STR(?subject))"
				+ " }" +
				"}";
	    queue(query);
	    instantiateWriter();
	    process(MethodHandles.lookup().lookupClass());
	    closeWriter();
	}
	
	// create the full merged index
	logger.info("");
	logger.info("merging subauthorities...");
	logger.info("");
	mergeSubauthorities();
   }
    

    int threadID = 0;
    
    public ISNIIndexer(int threadID) {
	logger.info("GeoNamesIndexer thread: " + threadID);
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
	    	indexISNI(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexISNI(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String name = null;
	String query = 
			" SELECT ?name where { " +
					"<" + URI + "> rdf:type schema:" + subauthority + " . " +
					"<" + URI + "> schema:alternateName ?name . "
					+ " FILTER NOT EXISTS {"
					+ "    <" + URI + "> schema:alternateName ?name2"
					+ "    FILTER (STRLEN(STR(?name2)) > STRLEN(STR(?name)) && STR(?name2) > STR(?name))"
					+ " }" +
					"}";
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
	theDocument.add(new TextField("prefcontent", retokenizeString(name, true), Field.Store.NO));

	    indexVariant(theDocument, URI, "schema:alternateName");
	    indexVariant(theDocument, URI, "rdf:type");
	    indexVariant(theDocument, URI, "rdfs:label");
	    indexVariant(theDocument, URI, "isni:hasDeprecatedISNI");
	    indexVariant(theDocument, URI, "mads:isIdentifiedByAuthority");
	    indexVariant(theDocument, URI, "dcterms:source");
	    switch(subauthority) {
	    case "Person":
		    indexVariant(theDocument, URI, "schema:birthDate");
		    indexVariant(theDocument, URI, "schema:deathDate");
	    	break;
	    case "Organization":
		    indexVariant(theDocument, URI, "schema:foundingDate");
	    	break;
	    }

	    indexVariant2(theDocument, URI, "schema:identifier","schema:value");
	    indexVariant2(theDocument, URI, "schema:identifier","schema:propertyID");

	    theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	if (++count % 100000 == 0)
	    logger.info("[" + threadID + "] count: " + count);
    }
    
    @SuppressWarnings("deprecation")
	void indexVariant(Document theDocument, String uri, String className) throws CorruptIndexException, IOException {
	String query =
		"SELECT DISTINCT ?name WHERE { "
		+ "<" + uri + "> " + className + " ?name . "
    		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String name = sol.get("?name").toString();
	    logger.debug("\t" + className + ": " + name);
	    
	    theDocument.add(new Field("content", name, Field.Store.NO, Field.Index.ANALYZED));
	}
    }
    
    @SuppressWarnings("deprecation")
	void indexVariant2(Document theDocument, String uri, String className1, String className2) throws CorruptIndexException, IOException {
	String query =
		"SELECT DISTINCT ?name WHERE { "
		+ "<" + uri + "> " + className1 + " ?v . "
		+ "?v " + className2 + " ?name . "
		    		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String name = sol.get("?name").toString();
	    logger.debug("\t" + className2 + ": " + name);
	    
	    theDocument.add(new Field("content", name, Field.Store.NO, Field.Index.ANALYZED));
	}
    }

}
