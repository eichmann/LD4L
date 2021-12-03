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

public class PCCIndexer extends ThreadedIndexer implements Runnable {
	static boolean deprecated = false;

	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.info");
		loadProperties("pcc");
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
			case "Work":
				query = "SELECT DISTINCT ?uri ?subject  WHERE { "
						+ "?uri rdf:type " + map(subauthority) + " . "
						+ "?uri bf:expressionOf ?x . " 
						+ "?x bf:title ?y . "
						+ "?y rdfs:label ?subject . "
						+ "} limit 100";
				break;
			case "Opus":
				query = "SELECT DISTINCT ?uri ?subject  WHERE { "
						+ "?uri rdf:type " + map(subauthority) + " . "
						+ "?uri bf:title ?y . "
						+ "?y rdfs:label ?subject . "
						+ "} limit 100";
				break;
			case "Instance":
				query = "SELECT DISTINCT ?uri ?subject  WHERE { "
						+ "?uri rdf:type " + map(subauthority) + " . "
						+ "?uri bf:title ?y . "
						+ "?y rdfs:label ?subject . "
						+ "} limit 100";
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

	static String map(String subauthority) {
		switch (subauthority) {
		case "Work":
			return "bf:Work";
		case "Opus":
			return "<https://share-vde.org/Opus>";
		case "Instance":
			return "bf:Instance";
		default:
			return "";
		}
	}

	int threadID = 0;

	public PCCIndexer(int threadID) {
		logger.info("PCCIndexer thread: " + threadID);
		this.threadID = threadID;
	}

	@Override
	public void run() {
		while (!uriQueue.isCompleted()) {
			String uri = uriQueue.dequeue();
			if (uri == null)
				return;
			logger.info("[" + threadID + "] indexing: " + uri);
//			try {
//				indexGetty(uri);
//			} catch (IOException | InterruptedException e) {
//				logger.error("Exception raised: " + e);
//			}
		}
	}

	void indexGetty(String URI) throws CorruptIndexException, IOException, InterruptedException {
		String name = null;
		String query = subauthority.equals(
				"aat") ? "SELECT DISTINCT ?name  WHERE { " + "<" + URI + "> rdf:type " + map(subauthority) + " . " + "  OPTIONAL { <" + URI + "> skos:prefLabel ?labelUS  FILTER (lang(?labelUS) = \"en-us\") } " + "  OPTIONAL { <" + URI + "> skos:prefLabel ?labelENG FILTER (lang(?labelENG) = \"en\") } " + "  OPTIONAL { <" + URI + "> skos:prefLabel ?labelNUL FILTER (lang(?labelNUL) = \"\") } " + "  OPTIONAL { <" + URI + "> skos:prefLabel ?labelANY FILTER (lang(?labelANY) != \"\") } " + "  BIND(COALESCE(?labelUS, ?labelENG, ?labelNUL, ?labelANY ) as ?name) " + "}" : "SELECT DISTINCT ?name  WHERE { " + "<" + URI + "> rdf:type " + map(subauthority) + " . " + "?urix foaf:focus <" + URI + "> . " + "  OPTIONAL { ?urix xl:prefLabel ?xl. ?xl xl:literalForm  ?labelUS  FILTER (lang(?labelUS) = \"en-us\") } " + "  OPTIONAL { ?urix xl:prefLabel ?xl. ?xl xl:literalForm  ?labelENG FILTER (lang(?labelENG) = \"en\") } " + "  OPTIONAL { ?urix xl:prefLabel ?xl. ?xl xl:literalForm  ?labelNUL FILTER (lang(?labelNUL) = \"\") } " + "  OPTIONAL { ?urix xl:prefLabel ?xl. ?xl xl:literalForm  ?labelANY FILTER (lang(?labelANY) != \"\") } " + "  BIND(COALESCE(?labelUS, ?labelENG, ?labelNUL, ?labelANY ) as ?name) " + "}";
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

		String query1 = subauthority.equals("aat")
				? "SELECT DISTINCT ?preflabel WHERE { " + "<" + URI + "> skos:prefLabel ?preflabel . " + "}"
				: "SELECT DISTINCT ?preflabel WHERE { " + "?urix foaf:focus <" + URI + "> . "
						+ "?urix xl:prefLabel ?xl. ?xl xl:literalForm ?preflabel . " + "}";
		ResultSet prs = getResultSet(prefix + query1);
		while (prs.hasNext()) {
			QuerySolution psol = prs.nextSolution();
			String preflabel = psol.get("?preflabel").asLiteral().getString();
			logger.info("\tpref label: " + preflabel);
			theDocument.add(new TextField("content", retokenizeString(preflabel, true), Field.Store.NO));
			theDocument.add(new TextField("prefcontent", retokenizeString(preflabel, true), Field.Store.NO));
		}

		String query2 = subauthority.equals("aat")
				? "SELECT DISTINCT ?altlabel WHERE { " + "<" + URI + "> skos:altLabel ?altlabel . " + "}"
				: "SELECT DISTINCT ?altlabel WHERE { " + "?urix foaf:focus <" + URI + "> . "
						+ "?urix xl:altLabel ?xl. ?xl xl:literalForm  ?altlabel . " + "}";
		ResultSet ars = getResultSet(prefix + query2);
		while (ars.hasNext()) {
			QuerySolution asol = ars.nextSolution();
			String altlabel = asol.get("?altlabel").asLiteral().getString();
			logger.info("\talt label: " + altlabel);
			theDocument.add(new TextField("content", retokenizeString(altlabel, true), Field.Store.NO));
		}

		if (subauthority.equals("tgn")) {
			String query3 = "SELECT DISTINCT ?label WHERE { " + "?urix foaf:focus <" + URI + "> . "
					+ "?urix gvp:parentString ?label . " + "}";
			ResultSet rs3 = getResultSet(prefix + query3);
			while (rs3.hasNext()) {
				QuerySolution asol = rs3.nextSolution();
				String altlabel = asol.get("?label").asLiteral().getString();
				logger.info("\tlabel1: " + altlabel);
				theDocument.add(new TextField("content", retokenizeString(altlabel, true), Field.Store.NO));
			}
			query3 = "SELECT DISTINCT ?label WHERE { " + "?urix foaf:focus <" + URI + "> . "
					+ "?urix gvp:parentStringAbbrev ?label . " + "}";
			rs3 = getResultSet(prefix + query3);
			while (rs3.hasNext()) {
				QuerySolution asol = rs3.nextSolution();
				String altlabel = asol.get("?label").asLiteral().getString();
				logger.info("\tlabel2: " + altlabel);
				theDocument.add(new TextField("content", retokenizeString(altlabel, true), Field.Store.NO));
			}
			query3 = "SELECT DISTINCT ?label WHERE { " + "?urix foaf:focus <" + URI + "> . "
					+ "?urix gvp:broaderPreferred ?x. " + "?x skos:prefLabel ?label . " + "}";
			rs3 = getResultSet(prefix + query3);
			while (rs3.hasNext()) {
				QuerySolution asol = rs3.nextSolution();
				String altlabel = asol.get("?label").asLiteral().getString();
				logger.info("\tlabel3: " + altlabel);
				theDocument.add(new TextField("content", retokenizeString(altlabel, true), Field.Store.NO));
			}
			query3 = "SELECT DISTINCT ?label WHERE { " + "?urix foaf:focus <" + URI + "> . "
					+ "?urix gvp:broaderPreferred ?x. " + "?x skos:altLabel ?label . " + "}";
			rs3 = getResultSet(prefix + query3);
			while (rs3.hasNext()) {
				QuerySolution asol = rs3.nextSolution();
				String altlabel = asol.get("?label").asLiteral().getString();
				logger.info("\tlabel4: " + altlabel);
				theDocument.add(new TextField("content", retokenizeString(altlabel, true), Field.Store.NO));
			}
		}

		theDocument.add(new StoredField("payload", generateSubauthorityPayload(URI, subauthority)));
		logger.debug("");

		theWriter.addDocument(theDocument);
		if (++count % 100000 == 0)
			logger.info("[" + threadID + "] count: " + count);
	}

}
