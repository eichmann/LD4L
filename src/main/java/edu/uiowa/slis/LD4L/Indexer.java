package edu.uiowa.slis.LD4L;

import java.io.File;
import java.io.IOException;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.tdb.TDBFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

public class Indexer {
    protected static Logger logger = Logger.getLogger(Indexer.class);
    
    static boolean useSPARQL = false;
    static Dataset dataset = null;
    static String tripleStore = null;
    static String endpoint = null;
    
    static String dataPath = "/Volumes/LD4L/";
    static String lucenePath = null;
    static String prefix = 
	    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
	    + " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
	    + " PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
	    + " PREFIX mads: <http://www.loc.gov/mads/rdf/v1#> "
	    + " PREFIX bib: <http://bib.ld4l.org/ontology/> ";

    
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException {
	PropertyConfigurator.configure("log4j.info");

	tripleStore = dataPath + args[0];
	endpoint = "http://guardian.slis.uiowa.edu:3030/" + args[0] + "/sparql";
	
	if (args.length > 0 && args[1].equals("work"))
	    lucenePath = dataPath + "lucene/" + args[0] + "/" + args[1];
	if (args.length > 0 && args[1].equals("person"))
	    lucenePath = dataPath + "lucene/" + args[0] + "/" + args[1];

	logger.info("data path: " + dataPath);
	logger.info("lucenePath: " + lucenePath);
	IndexWriter theWriter = new IndexWriter(FSDirectory.open(new File(lucenePath)), new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_30), true, IndexWriter.MaxFieldLength.UNLIMITED);
	
	if (args.length > 0 && args[1].equals("work"))
	    indexWorkTitles(theWriter);
	if (args.length > 0 && args[1].equals("person"))
	    indexPersons(theWriter);

	logger.info("optimizing index...");
	theWriter.optimize();
	theWriter.close();
    }
    
    static void indexWorkTitles(IndexWriter theWriter) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT DISTINCT ?work ?title WHERE { "
		+ "?work rdf:type bib:Work . "
		+ "?work bib:hasTitle ?x . "
		+ "?x rdfs:label ?title . "
    		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String work = sol.get("?work").toString();
	    String title = sol.get("?title").toString();
	    logger.debug("work: " + work + "\ttitle: " + title);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", work, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("title", title, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", title, Field.Store.NO, Field.Index.ANALYZED));
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total titles: " + count);
    }
    
    static void indexPersons(IndexWriter theWriter) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT DISTINCT ?puri ?name WHERE { "
		+ "?uri rdf:type mads:Authority . "
		+ "?uri mads:identifiesRWO ?puri . "
		+ "?uri mads:authoritativeLabel ?name . "
		+ "?puri rdf:type foaf:Person . "
    		+ "} ";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
//	    String authorityURI = sol.get("?uri").toString();
	    String personURI = sol.get("?puri").toString();
	    String name = sol.get("?name").toString();
	    logger.debug("uri: " + personURI + "\tname: " + name);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", personURI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", name, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", name, Field.Store.NO, Field.Index.ANALYZED));
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total titles: " + count);
    }

    static public ResultSet getResultSet(String queryString) {
	if (useSPARQL) {
	    Query theClassQuery = QueryFactory.create(queryString, Syntax.syntaxARQ);
	    QueryExecution theClassExecution = QueryExecutionFactory.sparqlService(endpoint, theClassQuery);
	    return theClassExecution.execSelect();
	} else {
	    dataset = TDBFactory.createDataset(tripleStore);
	    Query query = QueryFactory.create(queryString, Syntax.syntaxARQ);
	    QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
	    return qexec.execSelect();
	}
    }
}
