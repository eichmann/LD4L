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

public class GettyIndexer extends ThreadedIndexer implements Runnable {
    static boolean deprecated = false;

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	loadProperties("getty");
	String[] subauthorities = getSubauthorities();
	logger.info("subauthorities: " + arrayString(subauthorities));

	for (String subauthority : subauthorities) {
	    logger.info("");
	    logger.info("indexing subauthority " + subauthority);
	    logger.info("");
	    setSubauthority(subauthority);
	    String query = 
			"SELECT DISTINCT ?uri ?subject  WHERE { "
				+ "?uri rdf:type " + map(subauthority) + " . "
				+ "  OPTIONAL { ?uri skos:prefLabel ?labelUS  FILTER (lang(?labelUS) = \"en-us\") } "
				+ "  OPTIONAL { ?uri skos:prefLabel ?labelENG FILTER (lang(?labelENG) = \"en\") } "
				+ "  OPTIONAL { ?uri skos:prefLabel ?labelNUL FILTER (lang(?labelNUL) = \"\") } "
				+ "  OPTIONAL { ?uri skos:prefLabel ?labelANY FILTER (lang(?labelANY) != \"\") } "
				+ "  BIND(COALESCE(?labelUS, ?labelENG, ?labelNUL, ?labelANY ) as ?subject) "
				+ "}";
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
	logger.info("merging ULAN subauthorities...");
	logger.info("");
	resetSubauthorities("ulan_person|ulan_organization");
	setAltLucenePath(lucenePath + "_ulan");
	mergeSubauthorities();
    }
    
    static String map(String subauthority) {
	switch(subauthority) {
	case "aat":
	    return "getty:Concept";
	case "tgn":
	    return "getty:PhysPlaceConcept";
	case "ulan_person":
	    return "getty:PersonConcept";
	case "ulan_organization":
	    return "getty:GroupConcept";
	default:
	    return "";
	}
    }

    int threadID = 0;
    
    public GettyIndexer(int threadID) {
	logger.info("GettyIndexer thread: " + threadID);
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
		"SELECT DISTINCT ?name  WHERE { "
			+ "<" + URI + "> rdf:type " + map(subauthority) + " . "
			+ "  OPTIONAL { <" + URI + "> skos:prefLabel ?labelUS  FILTER (lang(?labelUS) = \"en-us\") } "
			+ "  OPTIONAL { <" + URI + "> skos:prefLabel ?labelENG FILTER (lang(?labelENG) = \"en\") } "
			+ "  OPTIONAL { <" + URI + "> skos:prefLabel ?labelNUL FILTER (lang(?labelNUL) = \"\") } "
			+ "  OPTIONAL { <" + URI + "> skos:prefLabel ?labelANY FILTER (lang(?labelANY) != \"\") } "
			+ "  BIND(COALESCE(?labelUS, ?labelENG, ?labelNUL, ?labelANY ) as ?name) "
			+ "}";
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
	if (++count % 100000 == 0)
	    logger.info("[" + threadID + "] count: " + count);
    }
    
}
