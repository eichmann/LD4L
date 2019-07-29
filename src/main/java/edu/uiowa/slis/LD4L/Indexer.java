package edu.uiowa.slis.LD4L;

import java.io.File;
import java.io.IOException;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
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
	    + " PREFIX owl: <http://www.w3.org/2002/07/owl#> "
	    + " PREFIX umls: <http://bioportal.bioontology.org/ontologies/umls/> "
	    + " PREFIX getty: <http://vocab.getty.edu/ontology#> "
	    + " PREFIX schema: <http://schema.org/> "
	    + " PREFIX mads: <http://www.loc.gov/mads/rdf/v1#> "
	    + " PREFIX bib: <http://bib.ld4l.org/ontology/> ";
    private static Pattern datePattern = Pattern.compile("([0-9]{4})-([0-9]{4})");

    
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException {
	PropertyConfigurator.configure("log4j.info");

	if (args.length == 1 && args[0].equals("agrovoc")) {
	    tripleStore = dataPath + "Agrovoc";
	    endpoint = "http://services.ld4l.org/fuseki/" + args[0] + "/sparql";
	} else if (args.length == 1 && args[0].equals("nalt")) {
	    tripleStore = dataPath + "NALT";
	    endpoint = "http://services.ld4l.org/fuseki/" + args[0] + "/sparql";
	} else if (args.length > 1 && (args[0].equals("loc") || args[0].equals("locRWO")) && !args[1].equals("subjects") && !args[1].equals("genre") && !args[1].equals("demographics") && !args[1].equals("performance") && !args[1].equals("work") && !args[1].equals("instance")) {
	    tripleStore = dataPath + "LoC/names";
	    endpoint = "http://services.ld4l.org/fuseki/loc_names/sparql";
	} else if (args.length > 1 && args[0].equals("loc") && args[1].equals("subjects")) {
	    tripleStore = dataPath + "LoC/subjects";
	    endpoint = "http://services.ld4l.org/fuseki/loc_subjects/sparql";
	} else if (args.length > 1 && args[0].equals("loc") && args[1].equals("genre")) {
	    tripleStore = dataPath + "LoC/genre";
	    endpoint = "http://services.ld4l.org/fuseki/loc_genre/sparql";
	} else if (args.length > 1 && args[0].equals("loc") && args[1].equals("demographics")) {
	    tripleStore = dataPath + "LoC/demographics";
	    endpoint = "http://services.ld4l.org/fuseki/loc_demographics/sparql";
	} else if (args.length > 1 && args[0].equals("loc") && args[1].equals("performance")) {
	    tripleStore = dataPath + "LoC/performance";
	    endpoint = "http://services.ld4l.org/fuseki/loc_performance/sparql";
	} else if (args.length > 1 && args[0].equals("loc") && args[1].equals("work")) {
	    tripleStore = dataPath + "LoC/works_instances";
	    endpoint = "http://services.ld4l.org/fuseki/loc_works_instances/sparql";
	} else if (args.length > 1 && args[0].equals("loc") && args[1].equals("instance")) {
	    tripleStore = dataPath + "LoC/works_instances";
	    endpoint = "http://services.ld4l.org/fuseki/loc_works_instances/sparql";
	} else if (args.length > 1 && args[0].equals("getty") && args[1].equals("aat")) {
	    tripleStore = dataPath + "Getty/AAT";
	    endpoint = "http://services.ld4l.org/fuseki/getty_aat/sparql";
	} else if (args.length > 1 && args[0].equals("getty") && args[1].equals("aat_facets")) {
	    tripleStore = dataPath + "Getty/AAT";
	    endpoint = "http://services.ld4l.org/fuseki/getty_aat/sparql";
	} else if (args.length > 1 && args[0].equals("getty") && args[1].equals("tgn")) {
	    tripleStore = dataPath + "Getty/TGN";
	    endpoint = "http://services.ld4l.org/fuseki/getty_tgn/sparql";
	} else if (args.length > 1 && args[0].equals("getty") && args[1].equals("ulan")) {
	    tripleStore = dataPath + "Getty/ULAN";
	    endpoint = "http://services.ld4l.org/fuseki/getty_ulan/sparql";
	} else if (args.length > 1 && args[0].equals("dbpedia")) {
	    tripleStore = dataPath + "dbpedia_2016-14";
	    endpoint = "http://services.ld4l.org/fuseki/dbpedia/sparql";
	} else if (args.length == 1 && args[0].equals("mesh")) {
	    tripleStore = dataPath + "MeSH";
	    endpoint = "http://services.ld4l.org/fuseki/mesh/sparql";
	} else if (args.length > 1 && args[0].equals("share_vde")) {
	    tripleStore = "/usr/local/RAID/LD4L/triplestores/share_vde/" + args[1];
	    endpoint = "http://services.ld4l.org/fuseki/share_vde_"+args[1]+"/sparql";
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
	    lucenePath = dataPath + "LD4L/lucene/loc/names";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("persons"))
	    lucenePath = dataPath + "LD4L/lucene/loc/persons";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("organizations"))
	    lucenePath = dataPath + "LD4L/lucene/loc/organizations";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("titles"))
	    lucenePath = dataPath + "LD4L/lucene/loc/titles";
	if (args.length > 1 && args[0].equals("locRWO") && args[1].equals("names"))
	    lucenePath = dataPath + "LD4L/lucene/locRWO/names";
	if (args.length > 1 && args[0].equals("locRWO") && args[1].equals("persons"))
	    lucenePath = dataPath + "LD4L/lucene/locRWO/persons";
	if (args.length > 1 && args[0].equals("locRWO") && args[1].equals("organizations"))
	    lucenePath = dataPath + "LD4L/lucene/locRWO/organizations";
	if (args.length > 1 && args[0].equals("locRWO") && args[1].equals("titles"))
	    lucenePath = dataPath + "LD4L/lucene/locRWO/titles";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("subjects"))
	    lucenePath = dataPath + "LD4L/lucene/loc/subjects";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("genre")) {
	    if (args[2].equals("deprecated"))
		lucenePath = dataPath + "LD4L/lucene/loc/genre_deprecated";
	    else
		lucenePath = dataPath + "LD4L/lucene/loc/genre_active";
	}
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("demographics"))
	    lucenePath = dataPath + "LD4L/lucene/loc/demographics";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("performance"))
	    lucenePath = dataPath + "LD4L/lucene/loc/performance";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("work"))
	    lucenePath = dataPath + "LD4L/lucene/loc/work";
	if (args.length > 1 && args[0].equals("loc") && args[1].equals("instance"))
	    lucenePath = dataPath + "LD4L/lucene/loc/instance";
	if (args.length > 1 && args[0].equals("getty") && args[1].equals("aat"))
	    lucenePath = dataPath + "LD4L/lucene/getty/aat";
	if (args.length > 1 && args[0].equals("getty") && args[1].equals("aat_facets"))
	    lucenePath = dataPath + "LD4L/lucene/getty/aat_facets";
	if (args.length > 1 && args[0].equals("getty") && args[1].equals("tgn"))
	    lucenePath = dataPath + "LD4L/lucene/getty/tgn";
	if (args.length == 2 && args[0].equals("getty") && args[1].equals("ulan"))
	    lucenePath = dataPath + "LD4L/lucene/getty/ulan";
	if (args.length > 2 && args[0].equals("getty") && args[1].equals("ulan") && args[2].equals("person"))
	    lucenePath = dataPath + "LD4L/lucene/getty/ulan_person";
	if (args.length > 2 && args[0].equals("getty") && args[1].equals("ulan") && args[2].equals("organization"))
	    lucenePath = dataPath + "LD4L/lucene/getty/ulan_organization";
	if (args.length > 1 && args[0].equals("dbpedia") && args[1].equals("person"))
	    lucenePath = dataPath + "LD4L/lucene/dbpedia/person";
	if (args.length > 1 && args[0].equals("dbpedia") && args[1].equals("work"))
	    lucenePath = dataPath + "LD4L/lucene/dbpedia/work";
	if (args.length > 1 && args[0].equals("dbpedia") && args[1].equals("organization"))
	    lucenePath = dataPath + "LD4L/lucene/dbpedia/organization";
	if (args.length > 1 && args[0].equals("dbpedia") && args[1].equals("place"))
	    lucenePath = dataPath + "LD4L/lucene/dbpedia/place";
	if (args.length > 1 && args[0].equals("share_vde") && args[2].equals("work"))
	    lucenePath = dataPath + "LD4L/lucene/share_vde/" + args[1] + "/work";
	if (args.length > 1 && args[0].equals("share_vde") && args[2].equals("superwork"))
	    lucenePath = dataPath + "LD4L/lucene/share_vde/" + args[1] + "/superwork";
	if (args.length > 1 && args[0].equals("share_vde") && args[2].equals("instance"))
	    lucenePath = dataPath + "LD4L/lucene/share_vde/" + args[1] + "/instance";
	if (args.length == 1 && args[0].equals("mesh"))
	    lucenePath = dataPath + "LD4L/lucene/mesh";

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
	if (args.length > 0 && args[0].equals("locRWO") && args[1].equals("names"))
	    indexLoCRWONames(theWriter, "");
	if (args.length > 0 && args[0].equals("locRWO") && args[1].equals("persons"))
	    indexLoCRWONames(theWriter, "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#PersonalName> . ");
	if (args.length > 0 && args[0].equals("locRWO") && args[1].equals("organizations"))
	    indexLoCRWONames(theWriter, "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#CorporateName> . ");
	if (args.length > 0 && args[0].equals("locRWO") && args[1].equals("titles"))
	    indexLoCRWONames(theWriter, "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#Title> . ");
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("subjects"))
	    indexLoCSubjects(theWriter);
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("genre"))
	    indexLoCGenre(theWriter, args[2].equals("deprecated"));
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("demographics"))
	    indexLoCDemographics(theWriter);
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("performance"))
	    indexLoCPerformance(theWriter);
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("work"))
	    indexLoCWorksInstances(theWriter, "Work");
	if (args.length > 0 && args[0].equals("loc") && args[1].equals("instance"))
	    indexLoCWorksInstances(theWriter, "Instance");
//	if (args.length > 0 && args[0].equals("loc") && args[1].equals("works_instances")) {
//	    lucenePath = dataPath + "LD4L/lucene/loc/work";
//	    indexLoCWorksInstances(theWriter, "Work");
//	    lucenePath = dataPath + "LD4L/lucene/loc/instance";
//	    indexLoCWorksInstances(theWriter, "Instance");
//	    Vector<String> requests = new Vector<String>();
//	    requests.add(dataPath + "LD4L/lucene/loc/work");
//	    requests.add(dataPath + "LD4L/lucene/loc/instance");
//	    mergeIndices(requests, dataPath + "LD4L/lucene/loc/works_instances");
//	}
	if (args.length > 0 && args[0].equals("getty") && args[1].equals("aat"))
	    indexGetty(theWriter, "getty:Concept");
	if (args.length > 0 && args[0].equals("getty") && args[1].equals("aat_facets"))
	    indexGettyAAT(theWriter, "getty:Concept");
	if (args.length > 0 && args[0].equals("getty") && args[1].equals("tgn"))
	    indexGetty(theWriter, "getty:PhysPlaceConcept");
	if (args.length == 2 && args[0].equals("getty") && args[1].equals("ulan")) {
	    indexGetty(theWriter, "getty:PersonConcept");
	    indexGetty(theWriter, "getty:GroupConcept");
	}
	if (args.length == 3 && args[0].equals("getty") && args[1].equals("ulan") && args[2].equals("person"))
	    indexGetty(theWriter, "getty:PersonConcept");
	if (args.length == 3 && args[0].equals("getty") && args[1].equals("ulan") && args[2].equals("organization"))
	    indexGetty(theWriter, "getty:GroupConcept");
	if (args.length > 0 && args[0].equals("dbpedia") && args[1].equals("person"))
	    indexDBpedia(theWriter, "Person");
	if (args.length > 0 && args[0].equals("dbpedia") && args[1].equals("work"))
	    indexDBpedia(theWriter, "Work");
	if (args.length > 0 && args[0].equals("dbpedia") && args[1].equals("organization"))
	    indexDBpedia(theWriter, "Organization");
	if (args.length > 0 && args[0].equals("dbpedia") && args[1].equals("place"))
	    indexDBpedia(theWriter, "Place");
	if (args.length > 0 && args[0].equals("share_vde") && args[2].equals("work"))
	    indexShareVDE(theWriter, "Work");
	if (args.length > 0 && args[0].equals("share_vde") && args[2].equals("superwork"))
	    indexShareVDE(theWriter, "SuperWork");
	if (args.length > 0 && args[0].equals("hare_vde") && args[2].equals("instance"))
	    indexShareVDE(theWriter, "Instance");
	if (args.length > 0 && args[0].equals("mesh"))
	    indexMeSH(theWriter);

	logger.info("optimizing index...");
	theWriter.optimize();
	theWriter.close();
    }
    
    static String retokenizeString(String originalQuery, boolean useDateHack) {
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

    static void indexShareVDE(IndexWriter theWriter, String entity) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		" SELECT DISTINCT ?s where { "+
		"  graph ?g { "+
		"    ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.loc.gov/ontologies/bibframe/" + entity + "> . "+
		"  } "+
		"}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    count++;
	    QuerySolution sol = rs.nextSolution();
	    String uri = sol.get("?s").toString();
	    if (count % 10000 == 0)
		logger.info("uri: " + uri);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", uri, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    
	    boolean first = true;
	    String query1 = 
		  "SELECT DISTINCT ?lab WHERE { "
			 + "  graph ?g { "
			  + "<" + uri + "> <http://id.loc.gov/ontologies/bibframe/title> ?x. "+
		"    ?x <http://www.w3.org/2000/01/rdf-schema#label> ?lab. "+
		"  } "+
		"}";
	    ResultSet ars = getResultSet(prefix + query1);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String name = asol.get("?lab").asLiteral().getString();
		if (count % 10000 == 0)
		    logger.info("\tname: " + name);
		if (first) {
		    theDocument.add(new Field("name", name, Field.Store.YES, Field.Index.NOT_ANALYZED));
		    first = false;
		}
		theDocument.add(new Field("content", name, Field.Store.NO, Field.Index.ANALYZED));
	    }

	    theWriter.addDocument(theDocument);
	    if (count % 10000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total " + entity + " count: " + count);
    }
    
    static void indexDBpedia(IndexWriter theWriter, String entity) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		" SELECT DISTINCT ?s ?lab where { "+
		"  ?s rdf:type <http://dbpedia.org/ontology/" + entity + "> . "+
		"  OPTIONAL { ?s rdfs:label ?labelUS  FILTER (lang(?labelUS) = \"en-US\") } "+
		"  OPTIONAL { ?s rdfs:label ?labelENG FILTER (langMatches(?labelENG,\"en\")) } "+
		"  OPTIONAL { ?s rdfs:label ?label    FILTER (lang(?label) = \"\") } "+
		"  OPTIONAL { ?s rdfs:label ?labelANY FILTER (lang(?labelANY) != \"\") } "+
		"  OPTIONAL { ?s <http://dbpedia.org/property/name> ?nameUS  FILTER (lang(?nameUS) = \"en-US\") } "+
		"  OPTIONAL { ?s <http://dbpedia.org/property/name> ?nameENG FILTER (langMatches(?nameENG,\"en\")) } "+
		"  OPTIONAL { ?s <http://dbpedia.org/property/name> ?name    FILTER (lang(?name) = \"\") } "+
		"  OPTIONAL { ?s <http://dbpedia.org/property/name> ?nameANY FILTER (lang(?nameANY) != \"\") } "+
		"  BIND(COALESCE(?labelUS, ?labelENG, ?label, ?labelANY, ?nameUS, ?nameENG, ?name, ?nameANY ) as ?lab) "+
		"}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String address = sol.get("?s").toString();
	    if (sol.get("?lab") == null) {
		logger.error("missing label for uri: " + address);
		continue;
	    }
	    String name = sol.get("?lab").asLiteral().getString();
	    logger.info("address: " + address + "\tname: " + name);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", address, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", name, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", retokenizeString(name, true), Field.Store.NO, Field.Index.ANALYZED));
	    
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 10000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total " + entity + " count: " + count);
    }
    
    static void indexMeSH(IndexWriter theWriter) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		" SELECT DISTINCT ?s ?lab ?def where { "+
		"  ?s rdf:type <http://id.nlm.nih.gov/mesh/vocab#Term> . "+
		"  ?s <http://id.nlm.nih.gov/mesh/vocab#prefLabel> ?lab . "+
		"  OPTIONAL { ?s skos:definition ?def } "+
		"}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String uri = sol.get("?s").toString();
	    if (sol.get("?lab") == null) {
		logger.error("missing label for uri: " + uri);
		continue;
	    }
	    String label = sol.get("?lab").asLiteral().getString();
	    String definition = sol.get("?def") == null ? null : sol.get("?def").asLiteral().getString();
	    logger.info("uri: " + uri + "\tlabel: " + label + "\tdefinition: " + definition);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", uri, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", label, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", label, Field.Store.NO, Field.Index.ANALYZED));
	    if (definition != null)
		theDocument.add(new Field("content", definition, Field.Store.NO, Field.Index.ANALYZED));

	    String query1 = 
		  "SELECT DISTINCT ?cui WHERE { "
			  + "<" + uri + "> umls:cui ?cui . "
		+ "}";
	    ResultSet ars = getResultSet(prefix + query1);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String cui = asol.get("?cui").asLiteral().getString();
		logger.info("\tcui: " + cui);
		theDocument.add(new Field("content", cui, Field.Store.NO, Field.Index.ANALYZED));
	    }
	    
	    String query2 = 
		  "SELECT DISTINCT ?altlabel WHERE { "
			  + "<" + uri + "> skos:altLabel ?altlabel . "
		+ "}";
	    ars = getResultSet(prefix + query2);
	    while (ars.hasNext()) {
		QuerySolution asol = ars.nextSolution();
		String altlabel = asol.get("?altlabel").asLiteral().getString();
		logger.info("\talt label: " + altlabel);
		theDocument.add(new Field("content", altlabel, Field.Store.NO, Field.Index.ANALYZED));
	    }
	    
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 10000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total count: " + count);
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
    
    static void indexGettyAAT(IndexWriter theWriter, String type) throws CorruptIndexException, IOException {
	int count = 0;
	String facetQuery = 
			"select ?s ?sl ?m ?ml where { "
			+ "  ?s <http://www.w3.org/2004/02/skos/core#member> ?m . "
			+ "  ?s <http://www.w3.org/2004/02/skos/core#prefLabel> ?sl . "
			+ "  ?m <http://www.w3.org/2004/02/skos/core#prefLabel> ?ml . "
			+ "  ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.getty.edu/ontology#Facet> . "
			+ "  FILTER (lang(?sl) = 'en' && lang(?ml) = 'en') "
			+ "  FILTER NOT EXISTS { ?s <http://vocab.getty.edu/ontology#broader> ?o2 } "
			+ "} ";
	ResultSet rs = getResultSet(prefix + facetQuery);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String facetURI = sol.get("?s").toString();
	    String facetLabel = sol.get("?sl").asLiteral().getString();
	    String rootURI = sol.get("?m").toString();
	    String rootLabel = sol.get("?ml").asLiteral().getString();
	    String facetID = "AAT" + facetURI.substring(facetURI.lastIndexOf('/')+1);
	    String rootID = "AAT" + rootURI.substring(rootURI.lastIndexOf('/')+1);
	    
	    logger.info("\nfacetURI: " + facetURI + "\tfacetLabel: " + facetLabel + "\trootURI: " + rootURI + "\trootLabel: " + rootLabel);
	    
	    String transQuery = 
		    	"select ?s ?sl where { "
		    	+ " ?s <http://vocab.getty.edu/ontology#broaderPreferred>* <" + rootURI + "> . "
		    	+ " ?s <http://www.w3.org/2004/02/skos/core#prefLabel> ?sl . "
		    	+ " FILTER (lang(?sl) = 'en') "
		    	+ " } ";
	    ResultSet trs = getResultSet(prefix + transQuery);
	    while (trs.hasNext()) {
		QuerySolution tsol = trs.nextSolution();
		String transURI = tsol.get("?s").toString();
		String transLabel = tsol.get("?sl").asLiteral().getString();

		logger.info("\ttransURI: " + transURI + "\ttransLabel: " + transLabel);
		Document theDocument = new Document();
		theDocument.add(new Field("uri", transURI, Field.Store.YES, Field.Index.NOT_ANALYZED));
		theDocument.add(new Field("title", transLabel, Field.Store.YES, Field.Index.NOT_ANALYZED));
		theDocument.add(new Field("content", transLabel, Field.Store.NO, Field.Index.ANALYZED));

		logger.info("\t\tfacetID: " + facetID);
		logger.info("\t\trootID: " + rootID);
		theDocument.add(new Field("content", facetID, Field.Store.NO, Field.Index.ANALYZED));
		theDocument.add(new Field("content", rootID, Field.Store.NO, Field.Index.ANALYZED));

		String query1 = "SELECT DISTINCT ?preflabel WHERE { " + "<" + transURI + "> skos:prefLabel ?preflabel . " + "}";
		ResultSet prs = getResultSet(prefix + query1);
		while (prs.hasNext()) {
		    QuerySolution psol = prs.nextSolution();
		    String preflabel = psol.get("?preflabel").asLiteral().getString();
		    logger.info("\t\tpref label: " + preflabel);
		    theDocument.add(new Field("content", preflabel, Field.Store.NO, Field.Index.ANALYZED));
		}

		String query2 = "SELECT DISTINCT ?altlabel WHERE { " + "<" + transURI + "> skos:altLabel ?altlabel . " + "}";
		ResultSet ars = getResultSet(prefix + query2);
		while (ars.hasNext()) {
		    QuerySolution asol = ars.nextSolution();
		    String altlabel = asol.get("?altlabel").asLiteral().getString();
		    logger.info("\t\talt label: " + altlabel);
		    theDocument.add(new Field("content", altlabel, Field.Store.NO, Field.Index.ANALYZED));
		}

		theWriter.addDocument(theDocument);
		count++;
		if (count % 100000 == 0)
		    logger.info("count: " + count);
	    }
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
	    String title = sol.get("?title").asLiteral().getString();
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
	    String name = sol.get("?name").asLiteral().getString();
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
	    String name = sol.get("?name").asLiteral().getString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.debug("uri: " + URI + "\tname: " + name);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", URI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", name, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", retokenizeString(name, true), Field.Store.NO, Field.Index.ANALYZED));
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
    
    static void indexLoCRWONames(IndexWriter theWriter, String typeConstraint) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT ?uri ?name ?rwo WHERE { "
		+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?name . "
		+ typeConstraint
		+ "?uri <http://www.loc.gov/mads/rdf/v1#identifiesRWO> ?rwo . "
    		+ "} ";
	logger.info("query: " + query);
	ResultSet rs = getResultSet(query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String RWO = sol.get("?rwo").toString();
	    String URI = sol.get("?uri").toString();
	    String name = sol.get("?name").asLiteral().getString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.debug("rwo: " + RWO + " uri: " + URI + "\tname: " + name);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", RWO, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", name, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", retokenizeString(name, true), Field.Store.NO, Field.Index.ANALYZED));
	    annotateLoCName(URI, theDocument, "hasVariant", "variantLabel");
//	    annotateLoCName(URI, theDocument, "fieldOfActivity", "label");
//	    annotateLoCName(URI, theDocument, "fieldOfActivity", "authoritativeLabel");
//	    annotateLoCName(URI, theDocument, "occupation", "authoritativeLabel");
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
	    String value = sol.get("?value").asLiteral().getString();
	    logger.debug("\tpredicate1: " + predicate1 + "\tvalue: " + value);
	    theDocument.add(new Field("content", retokenizeString(value, true), Field.Store.NO, Field.Index.ANALYZED));
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
	    String subject = sol.get("?subject").asLiteral().getString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.debug("uri: " + URI + "\tsubject: " + subject);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", URI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", subject, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", retokenizeString(subject, true), Field.Store.NO, Field.Index.ANALYZED));
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total names: " + count);
    }

    // <http://id.loc.gov/authorities/genreForms/gf2011026608> <http://www.loc.gov/mads/rdf/v1#adminMetadata> _:bnode11622439792158605647 .
    // _:bnode11622439792158605647 <http://id.loc.gov/ontologies/RecordInfo#recordStatus> "deprecated" .
    // _:bnode12837739696807266797 <http://id.loc.gov/ontologies/RecordInfo#recordStatus> "revised" .
    // _:bnode427471899577210033 <http://id.loc.gov/ontologies/RecordInfo#recordStatus> "new" .
    static void indexLoCGenre(IndexWriter theWriter, boolean deprecated) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT ?uri ?subject ?status WHERE { "
		+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
		+ "?uri <http://www.loc.gov/mads/rdf/v1#adminMetadata> ?stat . "
		+ "?stat <http://id.loc.gov/ontologies/RecordInfo#recordStatus> ?status . "
    		+ "} ";
	logger.info("triplestore: " + tripleStore);
	logger.info("query: " + query);
	ResultSet rs = getResultSet(query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String URI = sol.get("?uri").toString();
	    String subject = sol.get("?subject").asLiteral().getString();
	    String status = sol.get("?status").asLiteral().getString();
	    
	    if (!URI.startsWith("http:") || (deprecated && !status.equals("deprecated")) || (!deprecated && status.equals("deprecated")))
		continue;
	    
	    logger.info("uri: " + URI + "\tsubject: " + subject + "\tstatus: " + status);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", URI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", subject, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", retokenizeString(subject, true), Field.Store.NO, Field.Index.ANALYZED));

	    String query1 = 
		  "SELECT DISTINCT ?altlabel WHERE { "
			  + "<" + URI + "> skos:altLabel ?altlabel . "
		+ "}";
	    ResultSet prs = getResultSet(prefix + query1);
	    while (prs.hasNext()) {
		QuerySolution psol = prs.nextSolution();
		String altlabel = psol.get("?altlabel").asLiteral().getString();
		logger.info("\talt label: " + altlabel);
		theDocument.add(new Field("content", altlabel, Field.Store.NO, Field.Index.ANALYZED));
	    }
	    
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total names: " + count);
    }

    static void indexLoCDemographics(IndexWriter theWriter) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT ?uri ?subject WHERE { "
		+ "?uri skos:prefLabel ?subject . "
    		+ "} ";
	logger.info("triplestore: " + tripleStore);
	logger.info("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String URI = sol.get("?uri").toString();
	    String subject = sol.get("?subject").asLiteral().getString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.info("uri: " + URI + "\tsubject: " + subject);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", URI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", subject, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    for (int i = 0; i < 10; i++) // result weighting hack
		theDocument.add(new Field("content", retokenizeString(subject, true), Field.Store.NO, Field.Index.ANALYZED));

	    // subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.loc.gov/mads/rdf/v1#Authority>
	    
	    // http://www.w3.org/2004/02/skos/core#broader uri skos:prefLabel literal
	    // http://www.w3.org/2004/02/skos/core#narrower uri skos:prefLabel literal
	    // http://www.loc.gov/mads/rdf/v1#hasVariant blank <http://www.loc.gov/mads/rdf/v1#variantLabel> literal
	    // <http://www.loc.gov/mads/rdf/v1#hasEarlierEstablishedForm> blank <http://www.loc.gov/mads/rdf/v1#variantLabel> literal
	    //  <http://www.w3.org/2004/02/skos/core#related> uri skos:prefLabel literal
	    addWeightedField(
		theDocument,
		"SELECT DISTINCT ?altlabel WHERE { "
			  + "<" + URI + "> skos:altLabel ?altlabel . "
		+ "}",
		"altlabel",
		2);
	    addWeightedField(
		theDocument,
		"SELECT DISTINCT ?broader WHERE { "
			  + "<" + URI + "> <http://www.w3.org/2004/02/skos/core#broader> ?broadURI . "
			  + "?broadURI skos:prefLabel ?broader . "
		+ "}",
		"broader",
		1);
	    addWeightedField(
		theDocument,
		"SELECT DISTINCT ?narrower WHERE { "
			  + "<" + URI + "> <http://www.w3.org/2004/02/skos/core#narrower> ?narrowURI . "
			  + "?narrowURI skos:prefLabel ?narrower . "
		+ "}",
		"narrower",
		1);
	    
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total demographics: " + count);
    }
    
    static void addWeightedField(Document theDocument, String query, String label, int weight) {
	ResultSet prs = getResultSet(prefix + query);
	while (prs.hasNext()) {
	    QuerySolution psol = prs.nextSolution();
	    String altlabel = psol.get("?"+label).asLiteral().getString();
	    logger.info("\t" + label + ": " + altlabel + "\tweight: " + weight);
	    for (int i = 0; i < weight; i++)
		theDocument.add(new Field("content", retokenizeString(altlabel,true), Field.Store.NO, Field.Index.ANALYZED));
	}
    }

    static void indexLoCPerformance(IndexWriter theWriter) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT ?uri ?subject WHERE { "
		+ "?uri <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?subject . "
    		+ "} ";
	logger.info("triplestore: " + tripleStore);
	logger.info("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String URI = sol.get("?uri").toString();
	    String subject = sol.get("?subject").asLiteral().getString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.info("uri: " + URI + "\tsubject: " + subject);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", URI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", subject, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", retokenizeString(subject, true), Field.Store.NO, Field.Index.ANALYZED));

	    String query1 = 
		  "SELECT DISTINCT ?altlabel WHERE { "
			  + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#hasVariant> ?x . "
			  + "?x <http://www.loc.gov/mads/rdf/v1#variantLabel> ?altlabel . "
		+ "}";
	    ResultSet prs = getResultSet(prefix + query1);
	    while (prs.hasNext()) {
		QuerySolution psol = prs.nextSolution();
		String altlabel = psol.get("?altlabel").asLiteral().getString();
		logger.info("\talt label: " + altlabel);
		theDocument.add(new Field("content", altlabel, Field.Store.NO, Field.Index.ANALYZED));
	    }
	    
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total performance: " + count);
    }

    static void indexLoCWorksInstances(IndexWriter theWriter, String type) throws CorruptIndexException, IOException {
	int count = 0;
	String query =
		"SELECT ?uri ?subject WHERE { "
		+ "?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://id.loc.gov/ontologies/bibframe/"+type+"> . "
		+ "?uri <http://www.w3.org/2000/01/rdf-schema#label> ?subject . "
    		+ "} ";
	logger.info("triplestore: " + tripleStore);
	logger.info("query: " + query);
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String URI = sol.get("?uri").toString();
	    String subject = sol.get("?subject").asLiteral().getString();
	    
	    if (!URI.startsWith("http:"))
		continue;
	    
	    logger.debug("uri: " + URI + "\tsubject: " + subject);
	    
	    Document theDocument = new Document();
	    theDocument.add(new Field("uri", URI, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("name", subject, Field.Store.YES, Field.Index.NOT_ANALYZED));
	    theDocument.add(new Field("content", retokenizeString(subject, true), Field.Store.NO, Field.Index.ANALYZED));

//	    String query1 = 
//		  "SELECT DISTINCT ?altlabel WHERE { "
//			  + "<" + URI + "> <http://www.loc.gov/mads/rdf/v1#hasVariant> ?x . "
//			  + "?x <http://www.loc.gov/mads/rdf/v1#variantLabel> ?altlabel . "
//		+ "}";
//	    ResultSet prs = getResultSet(prefix + query1);
//	    while (prs.hasNext()) {
//		QuerySolution psol = prs.nextSolution();
//		String altlabel = psol.get("?altlabel").asLiteral().getString();
//		logger.info("\talt label: " + altlabel);
//		theDocument.add(new Field("content", altlabel, Field.Store.NO, Field.Index.ANALYZED));
//	    }
	    
	    theWriter.addDocument(theDocument);
	    count++;
	    if (count % 100000 == 0)
		logger.info("count: " + count);
	}
	logger.info("total performance: " + count);
    }

    static void indexVariant(Document theDocument, String uri, String className) throws CorruptIndexException, IOException {
	String query =
		"SELECT DISTINCT ?name WHERE { "
		+ "<" + uri + "> <http://www.geonames.org/ontology#" + className + "> ?name . "
    		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String name = sol.get("?name").asLiteral().getString();
	    logger.debug("\tclassName: " + className + "\tname: " + name);
	    
	    theDocument.add(new Field("content", name, Field.Store.NO, Field.Index.ANALYZED));
	}
    }
    
    static void indexVariant2(Document theDocument, String uri, String className) throws CorruptIndexException, IOException {
	String query =
		"SELECT DISTINCT ?name WHERE { "
		+ "<" + uri + "> <http://www.geonames.org/ontology#" + className + "> ?v . "
		+ "?v <http://www.geonames.org/ontology#name> ?name . "
		    		+ "}";
	ResultSet rs = getResultSet(prefix + query);
	while (rs.hasNext()) {
	    QuerySolution sol = rs.nextSolution();
	    String name = sol.get("?name").asLiteral().getString();
	    logger.debug("\tclassName: " + className + "\tname: " + name);
	    
	    theDocument.add(new Field("content", name, Field.Store.NO, Field.Index.ANALYZED));
	}
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

    @SuppressWarnings("deprecation")
    public static void mergeIndices(Vector<String> requests, String targetPath) throws CorruptIndexException, IOException {
	IndexWriterConfig config = new IndexWriterConfig(org.apache.lucene.util.Version.LUCENE_30, new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_30));
	config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
	config.setRAMBufferSizeMB(500);
	IndexWriter theWriter = new IndexWriter(FSDirectory.open(new File(targetPath)), config);

	logger.info("sites: " + requests);
	Directory indices[] = new Directory[requests.size()];
	for (int i = 0; i < requests.size(); i++) {
	    indices[i] = FSDirectory.open(new File(requests.elementAt(i)));
	}

	logger.info("merging indices...");
	logger.info("\tsource indices: " + requests);
	logger.info("\ttargetPath: " + targetPath);
	theWriter.addIndexes(indices);

	logger.info("optimizing index...");
	theWriter.optimize();
	theWriter.close();
	logger.info("done");
    }
}
