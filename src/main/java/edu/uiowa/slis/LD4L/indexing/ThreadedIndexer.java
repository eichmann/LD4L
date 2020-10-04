package edu.uiowa.slis.LD4L.indexing;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.tdb.TDBFactory;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uiowa.extraction.LocalProperties;
import edu.uiowa.extraction.PropertyLoader;
import edu.uiowa.lucene.ld4lSearch.LD4LAnalyzer;

public abstract class ThreadedIndexer {
    protected static Logger logger = Logger.getLogger(ThreadedIndexer.class);
    protected static LocalProperties prop_file = null;
    
    static Dataset dataset = null;
    protected static String tripleStore = null;
    protected static String tripleStorePath = null;
    static String endpoint = null;
    static boolean stemming = false;
    
    static String dataPath = null;
    protected static String lucenePath = null;
    static String altLucenePath = null;
    protected static String subauthority = null;
    private static Pattern datePattern = Pattern.compile("([0-9]{4})-([0-9]{4})");
    protected static String prefix =   "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
                        	    + " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        	    + " PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
                        	    + " PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
                        	    + " PREFIX owl: <http://www.w3.org/2002/07/owl#> "
                        	    + " PREFIX umls: <http://bioportal.bioontology.org/ontologies/umls/> "
                        	    + " PREFIX getty: <http://vocab.getty.edu/ontology#> "
                        	    + " PREFIX schema: <http://schema.org/> "
                        	    + " PREFIX mads: <http://www.loc.gov/mads/rdf/v1#> "
                        	    + " PREFIX loc: <http://id.loc.gov/vocabulary/identifiers/> "
                        	    + " PREFIX bib: <http://bib.ld4l.org/ontology/> "
                        	    + " PREFIX gn: <http://www.geonames.org/ontology#> ";
    				
    
    protected static IndexWriter theWriter = null;
    protected static Queue<String> uriQueue = new Queue<String>();
    protected static int count = 0;
    private static String[] subauthorities = null;

    protected static void loadProperties(String propertyFileName) {
	prop_file = PropertyLoader.loadProperties(propertyFileName);
	tripleStore = prop_file.getProperty("tripleStore");
	tripleStorePath = prop_file.getProperty("tripleStorePath");
	lucenePath = prop_file.getProperty("lucenePath");
	stemming = prop_file.getBooleanProperty("stemming");
	dataset = TDBFactory.createDataset(tripleStore);
    }
    
    protected static void resetSubauthorities(String subauthorityString) {
	subauthorities = subauthorityString.split("\\|");
    }
    
    protected static void setAltLucenePath(String altPath) {
	altLucenePath = altPath;
    }
    
    protected static String[] getSubauthorities() {
	if (subauthorities != null)
	    return subauthorities;
	String subauthorityString = prop_file.getProperty("subauthorities");
	if (subauthorityString != null)
	    subauthorities = subauthorityString.split("\\|");
	return subauthorities;
    }
    
    protected static boolean hasSubauthorities() {
	return subauthorities != null;
    }
    
    protected static void setSubauthority(String candidate) {
	subauthority = candidate;
	if (tripleStorePath != null)
	    dataset = TDBFactory.createDataset(tripleStorePath + "/" + subauthority);
    }
    
    protected static void instantiateWriter() throws IOException {
	IndexWriterConfig config = new IndexWriterConfig(org.apache.lucene.util.Version.LUCENE_43, new LD4LAnalyzer(stemming));
	config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
	config.setRAMBufferSizeMB(500);
	theWriter = new IndexWriter(FSDirectory.open(new File(lucenePath + (subauthority == null ? "" : "_" + subauthority))), config);
	logger.info("actual lucene path: " + lucenePath + (subauthority == null ? "" : "_" + subauthority));
    }
    
    protected static void closeWriter() throws IOException {
	theWriter.close();
    }
    
    protected static void mergeSubauthorities() throws CorruptIndexException, IOException {
	IndexWriterConfig config = new IndexWriterConfig(org.apache.lucene.util.Version.LUCENE_43, new LD4LAnalyzer());
	config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
	config.setRAMBufferSizeMB(500);
	IndexWriter theWriter = new IndexWriter(FSDirectory.open(new File(altLucenePath == null ? lucenePath : altLucenePath)), config);

	String[] requests = getSubauthorities();
	logger.info("sites: " + arrayString(requests));
	Directory indices[] = new Directory[requests.length];
	for (int i = 0; i < requests.length; i++) {
	    indices[i] = FSDirectory.open(new File(lucenePath+"_"+requests[i]));
	}

	logger.info("merging indices...");
	logger.info("\tsource indices: " + arrayString(requests));
	logger.info("\ttargetPath: " + (altLucenePath == null ? lucenePath : altLucenePath));
	theWriter.addIndexes(indices);

	theWriter.close();
	logger.info("done");
    }

    protected static void testing() {
	logger.info(MethodHandles.lookup().lookupClass());
    }
    
    protected static void queue(String query) {
	logger.info("triplestore: " + tripleStore);
	logger.info("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    
	    if (sol.get("?subject") == null)
		continue;
	    
	    String URI = sol.get("?uri").toString();
	    String subject = sol.get("?subject").asLiteral().getString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.debug("uri: " + URI + "\tsubject: " + subject);
	    uriQueue.queue(URI);
	}
    }
    
    protected static void process(Class<?> theClass) throws Exception {
	int maxCrawlerThreads = Runtime.getRuntime().availableProcessors();
//	int maxCrawlerThreads = 1;
	Thread[] scannerThreads = new Thread[maxCrawlerThreads];
	count =  0;

	for (int i = 0; i < maxCrawlerThreads; i++) {
	    logger.info("starting thread " + i);
	    Thread theThread = new Thread((Runnable) theClass.getConstructor(int.class).newInstance(i));
	    theThread.setPriority(Math.max(theThread.getPriority() - 2, Thread.MIN_PRIORITY));
	    theThread.start();
	    scannerThreads[i] = theThread;
	}

	for (int i = 0; i < maxCrawlerThreads; i++) {
	    scannerThreads[i].join();
	}
	logger.info("indexing completed: " + count + " instances.");
    }

    protected static void addWeightedField(Document theDocument, String query, String label, int weight) {
	ResultSet prs = getResultSet(prefix + query);
	while (prs.hasNext()) {
	    QuerySolution psol = prs.nextSolution();
	    String altlabel = psol.get("?"+label).asLiteral().getString();
	    logger.debug("\t" + label + ": " + altlabel + "\tweight: " + weight);
	    for (int i = 0; i < weight; i++)
		theDocument.add(new TextField("content", retokenizeString(altlabel,true), Field.Store.NO));
	}
    }

    static public ResultSet getResultSet(String queryString) {
	Query query = QueryFactory.create(queryString, Syntax.syntaxARQ);
	QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
	return qexec.execSelect();
    }

    protected static String retokenizeString(String originalQuery, boolean useDateHack) {
	StringBuffer buffer = new StringBuffer();

	for (String term : originalQuery.split("[, ]+")) {
	    if (buffer.length() > 0)
		buffer.append(" ");
	    if (useDateHack) {
		Matcher dateMatcher = datePattern.matcher(term);
		if (dateMatcher.matches()) {
		    buffer.append(dateMatcher.group(1) + " " + dateMatcher.group(2));
		} else
		    buffer.append(term);
	    } else {
		buffer.append(term);		
	    }
	}
	
	return buffer.toString().trim();
    }
    
    protected static String arrayString(String[] array) {
	if (array == null)
	    return null;

	StringBuffer result = new StringBuffer("[");
	for (int i = 0; i < array.length; i++)
	    result.append(" " + array[i]);
	result.append(" ]");
	return result.toString();
    }
    
    static int payloadCount = 0;
    protected static String generatePayload(String queryIRI) throws MalformedURLException, IOException, InterruptedException {
	StringBuffer buffer = new StringBuffer();

	ParameterizedSparqlString parameterizedString = new ParameterizedSparqlString(prefix + prop_file.getProperty("query"));
	logger.debug("query: " + parameterizedString.toString());
	parameterizedString.setIri(prop_file.getProperty("queryVariable"), queryIRI);
	logger.debug("parameterized query: " + parameterizedString.toString());
	Query theQuery = parameterizedString.asQuery();
	QueryExecution qexec = QueryExecutionFactory.create(theQuery, dataset);
	Model model = qexec.execConstruct();
	model.listStatements();
	for (Iterator<Statement> i = model.listStatements(); i.hasNext(); ) {
	    Triple triple = i.next().asTriple();
	    String tripleString = formatNode(triple.getSubject())  + " " + formatNode(triple.getPredicate()) + " " + formatNode(triple.getObject()) + " .";
	    logger.debug("triple: " + tripleString);
	    buffer.append(tripleString + "\n");
	}
	
	return buffer.toString();
    }
    
    static Hashtable<String,String> blankNodeHash = new Hashtable<String,String>();
    static int blankNodeCount = 0;
    
    private static String formatNode(Node node) {
	if (node.isLiteral()) {
	    return "\""+node.getLiteral().toString().replace("\"", "\\\"").replace("\n", "\\n")+"\""+(node.getLiteral().language() == null || node.getLiteral().language().trim().length() == 0 ? "" : "@"+node.getLiteral().language().toLowerCase());
	} else if (node.isBlank()) {
	    return getBlankNodeLabel(node.toString());
	} else {
	    return "<"+node.toString()+">";
	}
    }
    
    private static String getBlankNodeLabel(String nodeLabel) {
	String blankLabel = blankNodeHash.get(nodeLabel);
	
	if (blankLabel == null) {
	    blankLabel = "_:b" + blankNodeCount++;
	    blankNodeHash.put(nodeLabel, blankLabel);
	}
	
	return blankLabel;
    }
 
    protected static void annotateLoCName(String URI, Document theDocument, String predicate1, String predicate2) {
	String query =
		"SELECT ?value WHERE { "
		+ "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#" + predicate1 + "> ?o . "
		+ "?o <http://www.loc.gov/mads/rdf/v1#" + predicate2 + "> ?value . "
    		+ "} ";
	logger.debug("query: " + query);
	ResultSet rs = getResultSet(query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String value = sol.get("?value").asLiteral().getString();
	    logger.debug("\tpredicate1: " + predicate1 + "\tvalue: " + value);
	    theDocument.add(new TextField("content", retokenizeString(value, true), Field.Store.NO));
	}	
    }

}
