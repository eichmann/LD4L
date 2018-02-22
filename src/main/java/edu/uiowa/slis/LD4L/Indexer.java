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
    
    static String dataPath = "/usr/local/RAID/";
    static String lucenePath = null;
    static String prefix = 
	    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
	    + " PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
	    + " PREFIX foaf: <http://xmlns.com/foaf/0.1/> "
	    + " PREFIX skos: <http://www.w3.org/2004/02/skos/core#> "
	    + " PREFIX getty: <http://vocab.getty.edu/ontology#> "
	    + " PREFIX schema: <http://schema.org/> "
	    + " PREFIX mads: <http://www.loc.gov/mads/rdf/v1#> "
	    + " PREFIX bib: <http://bib.ld4l.org/ontology/> ";

    
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException {
	PropertyConfigurator.configure("log4j.info");

	if (args.length == 1 && args[0].equals("agrovoc")) {
	    tripleStore = dataPath + "Agrovoc";
	    endpoint = "http://services.ld4l.org/fuseki/" + args[0] + "/sparql";
	} else if (args.length == 1 && args[0].equals("nalt")) {
	    tripleStore = dataPath + "NALT";
	    endpoint = "http://services.ld4l.org/fuseki/" + args[0] + "/sparql";
	} else if (args.length > 1 && args[0].equals("loc") && !args[1].equals("subjects") && !args[1].equals("genre")) {
	    tripleStore = dataPath + "LoC/names";
	    endpoint = "http://services.ld4l.org/fuseki/loc_names/sparql";
	} else if (args.length > 1 && args[0].equals("loc") && args[1].equals("subjects")) {
	    tripleStore = dataPath + "LoC/subjects";
	    endpoint = "http://services.ld4l.org/fuseki/loc_subjects/sparql";
	} else if (args.length > 1 && args[0].equals("loc") && args[1].equals("genre")) {
	    tripleStore = dataPath + "LoC/genre";
	    endpoint = "http://services.ld4l.org/fuseki/loc_genre/sparql";
	} else if (args.length > 1 && args[0].equals("getty") && args[1].equals("aat")) {
	    tripleStore = dataPath + "Getty/AAT";
	    endpoint = "http://services.ld4l.org/fuseki/getty_aat/sparql";
	} else if (args.length > 1 && args[0].equals("getty") && args[1].equals("tgn")) {
	    tripleStore = dataPath + "Getty/TGN";
	    endpoint = "http://services.ld4l.org/fuseki/getty_tgn/sparql";
	} else if (args.length > 1 && args[0].equals("getty") && args[1].equals("ulan")) {
	    tripleStore = dataPath + "Getty/ULAN";
	    endpoint = "http://services.ld4l.org/fuseki/getty_ulan/sparql";
	} else {
	    tripleStore = dataPath + args[0];
	    endpoint = "http://services.ld4l.org/fuseki/" + args[0] + "/sparql";
	}
	
	if (args.length == 1 && args[0].equals("agrovoc"))
	    lucenePath = dataPath + "LD4L/lucene/" + args[0] + "/";
	else if (args.length == 1 && args[0].equals("nalt"))
	    lucenePath = dataPath + "LD4L/lucene/" + args[0] + "/";
	else if (args.length > 1 && args[1].equals("work"))
	    lucenePath = dataPath + "lucene/" + args[0] + "/" + args[1];
	if (args.length > 1 && args[1].equals("person"))
	    lucenePath = dataPath + "lucene/" + args[0] + "/" + args[1];
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("names"))
	    lucenePath = dataPath + "lucene/loc/names";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("persons"))
	    lucenePath = dataPath + "lucene/loc/persons";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("organizations"))
	    lucenePath = dataPath + "lucene/loc/organizations";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("titles"))
	    lucenePath = dataPath + "lucene/loc/titles";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("subjects"))
	    lucenePath = dataPath + "lucene/loc/subjects";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("genre"))
	    lucenePath = dataPath + "lucene/loc/genre";
	if (args.length > 1 && args[0].equals("getty") && args[1].equals("aat"))
	    lucenePath = dataPath + "LD4L/lucene/getty/aat";
	if (args.length > 1 && args[0].equals("getty") && args[1].equals("tgn"))
	    lucenePath = dataPath + "LD4L/lucene/getty/tgn";
	if (args.length > 2 && args[0].equals("getty") && args[1].equals("ulan") && args[2].equals("person"))
	    lucenePath = dataPath + "LD4L/lucene/getty/ulan_person";
	if (args.length > 2 && args[0].equals("getty") && args[1].equals("ulan") && args[2].equals("organization"))
	    lucenePath = dataPath + "LD4L/lucene/getty/ulan_organization";

	logger.info("endpoint: " + endpoint);
	logger.info("triplestore: " + tripleStore);
	logger.info("lucenePath: " + lucenePath);
	IndexWriter theWriter = new IndexWriter(FSDirectory.open(new File(lucenePath)), new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_30), true, IndexWriter.MaxFieldLength.UNLIMITED);
	
	if (args.length == 1 && args[0].equals("agrovoc"))
	    indexAgrovoc(theWriter);
	if (args.length == 1 && args[0].equals("nalt"))
	    indexNALT(theWriter);
	if (args.length > 1 && args[1].equals("work"))
	    indexWorkTitles(theWriter);
	if (args.length > 1 && args[1].equals("person"))
	    indexPersons(theWriter);
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("names"))
	    indexLoCNames(theWriter, "");
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("persons"))
	    indexLoCNames(theWriter, "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#PersonalName> . ");
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("organizations"))
	    indexLoCNames(theWriter, "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#CorporateName> . ");
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("titles"))
	    indexLoCNames(theWriter, "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#Title> . ");
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("subjects"))
	    indexLoCSubjects(theWriter);
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("genre"))
	    indexLoCGenre(theWriter);
	if (args.length > 0 && args[0].equals("getty") && args[1].equals("aat"))
	    indexGetty(theWriter, "getty:Concept");
	if (args.length > 0 && args[0].equals("getty") && args[1].equals("tgn"))
	    indexGetty(theWriter, "getty:PhysPlaceConcept");
	if (args.length > 0 && args[0].equals("getty") && args[1].equals("ulan") && args[2].equals("person"))
	    indexGetty(theWriter, "getty:PersonConcept");
	if (args.length > 0 && args[0].equals("getty") && args[1].equals("ulan") && args[2].equals("organization"))
	    indexGetty(theWriter, "getty:GroupConcept");

	logger.info("optimizing index...");
	theWriter.optimize();
	theWriter.close();
    }
    
    static void indexGetty(IndexWriter theWriter, String type) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT DISTINCT ?uri ?label  WHERE { "
		+ "?uri rdf:type " + type + " . "
		+ "  OPTIONAL { ?uri skos:prefLabel ?labelUS  FILTER (lang(?labelUS) = \"en-us\") } "
		+ "  OPTIONAL { ?uri skos:prefLabel ?labelENG FILTER (lang(?labelENG) = \"en\") } "
		+ "  OPTIONAL { ?uri skos:prefLabel ?labelNUL FILTER (lang(?labelNUL) = \"\") } "
		+ "  OPTIONAL { ?uri skos:prefLabel ?labelANY FILTER (lang(?labelANY) != \"\") } "
		+ "  BIND(COALESCE(?labelUS, ?labelENG, ?labelNUL, ?labelANY ) as ?label) "
		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String uri = sol.get("?uri").toString();
	    
	    if (sol.get("?label") == null)
		continue;
	    
	    String label = sol.get("?label").asLiteral().getString();
	    logger.info("uri: " + uri + "\tlabel: " + label);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", uri, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("title", label, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", label, Field.Store.NO, Field.Index.ANALYZED));
	    
	    String query1 = 
		  "SELECT DISTINCT ?preflabel WHERE { "
			  + "<" + uri + "> skos:prefLabel ?preflabel . "
		+ "}";
	    ResultSet prs = getResultSet(prefix + query1);
	    while (prs.hasNext()) {
		QuerySolution psol = prs.nextSolution();
		String preflabel = psol.get("?preflabel").asLiteral().getString();
		logger.info("\tpref label: " + preflabel);
		theDocument.add(new Field("content", preflabel, Field.Store.NO, Field.Index.ANALYZED));
	    }
	    
	    String query2 = 
		  "SELECT DISTINCT ?altlabel WHERE { "
			  + "<" + uri + "> skos:altLabel ?altlabel . "
		+ "}";
	    ResultSet ars = getResultSet(prefix + query2);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String altlabel = asol.get("?altlabel").asLiteral().getString();
		logger.info("\talt label: " + altlabel);
		theDocument.add(new Field("content", altlabel, Field.Store.NO, Field.Index.ANALYZED));
	    }
	    
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total concepts: " + count);
    }
    
    static void indexAgrovoc(IndexWriter theWriter) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT DISTINCT ?uri ?label  WHERE { "
		+ "?uri rdf:type skos:Concept . "
		+ "?uri skos:prefLabel ?label . "
		+ "FILTER (lang(?label) = 'en') "
    		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String uri = sol.get("?uri").toString();
	    String label = sol.get("?label").asLiteral().getString();
	    logger.info("uri: " + uri + "\tlabel: " + label);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", uri, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("title", label, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", label, Field.Store.NO, Field.Index.ANALYZED));
	    
	    String query1 = 
		  "SELECT DISTINCT ?preflabel WHERE { "
			  + "<" + uri + "> skos:prefLabel ?preflabel . "
		+ "}";
	    ResultSet prs = getResultSet(prefix + query1);
	    while (prs.hasNext()) {
		QuerySolution psol = prs.nextSolution();
		String preflabel = psol.get("?preflabel").asLiteral().getString();
		logger.info("\tpref label: " + preflabel);
		theDocument.add(new Field("content", preflabel, Field.Store.NO, Field.Index.ANALYZED));
	    }
	    
	    String query2 = 
		  "SELECT DISTINCT ?altlabel WHERE { "
			  + "<" + uri + "> skos:altLabel ?altlabel . "
		+ "}";
	    ResultSet ars = getResultSet(prefix + query2);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String altlabel = asol.get("?altlabel").asLiteral().getString();
		logger.info("\talt label: " + altlabel);
		theDocument.add(new Field("content", altlabel, Field.Store.NO, Field.Index.ANALYZED));
	    }
	    
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total concepts: " + count);
    }
    
    static void indexNALT(IndexWriter theWriter) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT DISTINCT ?uri ?label ?spanish WHERE { "
		+ "?uri rdf:type skos:Concept . "
		+ "?uri skos:prefLabel ?label . "
		+ "?uri skos:prefLabel ?spanish . "
		+ "FILTER (lang(?label) = 'en') "
		+ "FILTER (lang(?spanish) = 'es') "
    		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String uri = sol.get("?uri").toString();
	    String label = sol.get("?label").asLiteral().getString();
	    String spanish = sol.get("?spanish").asLiteral().getString();
	    logger.info("uri: " + uri + "\tlabel: " + label + "\tspanish: " + spanish);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", uri, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("title", label, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", label, Field.Store.NO, Field.Index.ANALYZED));
	    theDocument.add(new Field("content", spanish, Field.Store.NO, Field.Index.ANALYZED));
	    
	    String query2 = 
		  "SELECT DISTINCT ?altlabel WHERE { "
			  + "<" + uri + "> skos:altLabel ?altlabel . "
		+ "}";
	    ResultSet ars = getResultSet(prefix + query2);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String altlabel = asol.get("?altlabel").asLiteral().getString();
		logger.info("\talt label: " + altlabel);
		theDocument.add(new Field("content", altlabel, Field.Store.NO, Field.Index.ANALYZED));
	    }
	    
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total concepts: " + count);
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
	logger.info("total persons: " + count);
    }

    static void indexLoCNames(IndexWriter theWriter, String typeConstraint) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT ?uri ?name WHERE { "
		+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		+ typeConstraint
    		+ "} ";
	logger.info("query: " + query);
	ResultSet rs = getResultSet(query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String URI = sol.get("?uri").toString();
	    String name = sol.get("?name").toString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.info("uri: " + URI + "\tname: " + name);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", URI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", name, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", name, Field.Store.NO, Field.Index.ANALYZED));
	    annotateLoCName(URI, theDocument, "hasVariant", "variantLabel");
	    annotateLoCName(URI, theDocument, "fieldOfActivity", "label");
	    annotateLoCName(URI, theDocument, "fieldOfActivity", "authoritativeLabel");
	    annotateLoCName(URI, theDocument, "occupation", "authoritativeLabel");
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total names: " + count);
    }
    
    static void annotateLoCName(String URI, Document theDocument, String predicate1, String predicate2) {
	String query =
		"SELECT ?value WHERE { "
		+ "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#" + predicate1 + "> ?o . "
		+ "?o <http://www.loc.gov/mads/rdf/v1#" + predicate2 + "> ?value . "
    		+ "} ";
	logger.debug("query: " + query);
	ResultSet rs = getResultSet(query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String value = sol.get("?value").toString();
	    logger.info("\tpredicate1: " + predicate1 + "\tvalue: " + value);
	    theDocument.add(new Field("content", value, Field.Store.NO, Field.Index.ANALYZED));
	}	
    }

    static void indexLoCSubjects(IndexWriter theWriter) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT ?uri ?subject WHERE { "
		+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
    		+ "} ";
	logger.debug("query: " + query);
	ResultSet rs = getResultSet(query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String URI = sol.get("?uri").toString();
	    String subject = sol.get("?subject").toString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.debug("uri: " + URI + "\tsubject: " + subject);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", URI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", subject, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", subject, Field.Store.NO, Field.Index.ANALYZED));
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total names: " + count);
    }

    static void indexLoCGenre(IndexWriter theWriter) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT ?uri ?subject WHERE { "
		+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
    		+ "} ";
	logger.info("triplestore: " + tripleStore);
	logger.info("query: " + query);
	ResultSet rs = getResultSet(query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String URI = sol.get("?uri").toString();
	    String subject = sol.get("?subject").toString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.info("uri: " + URI + "\tsubject: " + subject);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", URI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", subject, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", subject, Field.Store.NO, Field.Index.ANALYZED));
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total names: " + count);
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
