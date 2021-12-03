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

	for (String subauthority : subauthorities) {
	    if (args.length > 0 && !args[0].equals(subauthority))
		continue;
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
	if (args.length == 0 || (args.length > 0 && args[0].equals("-merge"))) {
	    logger.info("");
	    logger.info("merging subauthorities...");
	    logger.info("");
	    mergeSubauthorities();
	}
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
	String name = null;
	String query = null;
	switch (subauthority) {
	case "active":
	    query = "SELECT ?subject WHERE { "
		    + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
		    + "} ";
	    break;
	case "deprecated":
	    query = "SELECT ?subject WHERE { "
		    + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#variantLabel> ?subject . "
		    + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#adminMetadata> ?stat . "
		    + "?stat <http://id.loc.gov/ontologies/RecordInfo#recordStatus> \"deprecated\" . "
		    + "} ";
	    break;
	}
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
