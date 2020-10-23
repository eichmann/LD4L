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

public class GeoNamesIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;
    static boolean strictMode = false;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("geonames");
	String[] subauthorities = getSubauthorities();
	logger.info("subauthorities: " + arrayString(subauthorities));

	    /*
	     * GeoNames Feature Codes:
	     * 		A - country, state, region, ...
	     * 		H - stream, lake, ...
	     * 		L - park, area, ...
	     * 		P - city, village, ...
	     * 		R - road, railroad
	     * 		S - spot, building, farm
	     * 		T - mountain, hill, rock, ...
	     * 		U - undersea
	     * 		V - forest, heath, ...
	     */
	for (String subauthority : subauthorities) {
	    logger.info("");
	    logger.info("indexing subauthority " + subauthority);
	    logger.info("");
	    setSubauthority(subauthority);
	    String query = 
			" SELECT ?uri ?subject where { " +
				"  ?uri rdf:type <http://www.geonames.org/ontology#Feature> . " +
				"  ?uri <http://www.geonames.org/ontology#featureClass> <http://www.geonames.org/ontology#" + subauthority + ">" + 
				"  OPTIONAL { ?uri rdfs:label ?labelUS  FILTER (lang(?labelUS) = \"en-US\") } " +
				"  OPTIONAL { ?uri rdfs:label ?labelENG FILTER (langMatches(?labelENG,\"en\")) } " +
				"  OPTIONAL { ?uri rdfs:label ?label    FILTER (lang(?label) = \"\") } " +
				"  OPTIONAL { ?uri rdfs:label ?labelANY FILTER (lang(?labelANY) != \"\") } " +
				"  OPTIONAL { ?uri <http://www.geonames.org/ontology#name> ?altLabel } " +
				"  BIND(COALESCE(?labelUS, ?labelENG, ?label, ?labelANY , ?altLabel) as ?subject) " +
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
	
	// create the ULAN-specific index
	logger.info("");
	logger.info("merging AP subauthorities...");
	logger.info("");
	resetSubauthorities("A|P");
	setAltLucenePath(lucenePath + "_AP");
	mergeSubauthorities();
    }
    

    int threadID = 0;
    
    public GeoNamesIndexer(int threadID) {
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
		indexGetty(uri);
	    } catch (IOException | InterruptedException e) {
		logger.error("Exception raised: " + e);
	    }
	}
    }
    
    void indexGetty(String URI) throws CorruptIndexException, IOException, InterruptedException {
	String name = null;
	String query = 
		" SELECT ?name where { " +
			"<" + URI + "> rdf:type <http://www.geonames.org/ontology#Feature> . " +
			"<" + URI + "> <http://www.geonames.org/ontology#featureClass> <http://www.geonames.org/ontology#" + subauthority + ">" + 
			"  OPTIONAL { <" + URI + "> rdfs:label ?labelUS  FILTER (lang(?labelUS) = \"en-US\") } " +
			"  OPTIONAL { <" + URI + "> rdfs:label ?labelENG FILTER (langMatches(?labelENG,\"en\")) } " +
			"  OPTIONAL { <" + URI + "> rdfs:label ?label    FILTER (lang(?label) = \"\") } " +
			"  OPTIONAL { <" + URI + "> rdfs:label ?labelANY FILTER (lang(?labelANY) != \"\") } " +
			"  OPTIONAL { <" + URI + "> <http://www.geonames.org/ontology#name> ?altLabel } " +
			"  BIND(COALESCE(?labelUS, ?labelENG, ?label, ?labelANY , ?altLabel) as ?name) " +
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
	theDocument.add(new TextField("content", retokenizeString(name, true), Field.Store.NO));
	theDocument.add(new TextField("prefcontent", retokenizeString(name, true), Field.Store.NO));

	    indexVariant(theDocument, URI, "alternateName");
	    indexVariant(theDocument, URI, "officialName");
	    if (!strictMode) {
		indexVariant2(theDocument, URI, "parentCountry");
		indexVariant2(theDocument, URI, "parentFeature");
		indexVariant2(theDocument, URI, "parentADM1");
		indexVariant2(theDocument, URI, "parentADM2");
		indexVariant2(theDocument, URI, "parentADM3");
		indexVariant2(theDocument, URI, "parentADM4");
	    }

	    theDocument.add(new StoredField("payload", generatePayload(URI)));

	theWriter.addDocument(theDocument);
	if (++count % 100000 == 0)
	    logger.info("[" + threadID + "] count: " + count);
    }
    
    void indexVariant(Document theDocument, String uri, String className) throws CorruptIndexException, IOException {
	String query =
		"SELECT DISTINCT ?name WHERE { "
		+ "<" + uri + "> <http://www.geonames.org/ontology#" + className + "> ?name . "
    		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String name = sol.get("?name").toString();
	    logger.debug("\tclassName: " + className + "\tname: " + name);
	    
	    theDocument.add(new Field("content", name, Field.Store.NO, Field.Index.ANALYZED));
	}
    }
    
    void indexVariant2(Document theDocument, String uri, String className) throws CorruptIndexException, IOException {
	String query =
		"SELECT DISTINCT ?name WHERE { "
		+ "<" + uri + "> <http://www.geonames.org/ontology#" + className + "> ?v . "
		+ "?v <http://www.geonames.org/ontology#name> ?name . "
		    		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String name = sol.get("?name").toString();
	    logger.debug("\tclassName: " + className + "\tname: " + name);
	    
	    theDocument.add(new Field("content", name, Field.Store.NO, Field.Index.ANALYZED));
	}
    }

}
