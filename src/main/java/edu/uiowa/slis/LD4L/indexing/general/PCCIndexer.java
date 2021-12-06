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
						+ "}";
				break;
			case "Opus":
				query = "SELECT DISTINCT ?uri ?subject  WHERE { "
						+ "?uri rdf:type " + map(subauthority) + " . "
						+ "?uri bf:title ?y . "
						+ "?y rdfs:label ?subject . "
						+ "}";
				break;
			case "Instance":
				query = "SELECT DISTINCT ?uri ?subject  WHERE { "
						+ "?uri rdf:type " + map(subauthority) + " . "
						+ "?uri bf:title ?y . "
						+ "?y rdfs:label ?subject . "
						+ "}";
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
			try {
				indexPCC(uri);
			} catch (IOException | InterruptedException e) {
				logger.error("Exception raised: " + e);
			}
		}
	}

	void indexPCC(String URI) throws CorruptIndexException, IOException, InterruptedException {
		String name = null;
		String query = null;
		switch (subauthority) {
		case "Work":
			query = "SELECT DISTINCT ?name  WHERE { "
					+ "<" + URI + "> rdf:type " + map(subauthority) + " . "
					+ "<" + URI + "> bf:expressionOf ?x . " 
					+ "?x bf:title ?y . "
					+ "?y rdfs:label ?name . "
					+ "}";
			break;
		case "Opus":
			query = "SELECT DISTINCT ?name  WHERE { "
					+ "<" + URI + "> rdf:type " + map(subauthority) + " . "
					+ "<" + URI + "> bf:title ?y . "
					+ "?y rdfs:label ?name . "
					+ "}";
			break;
		case "Instance":
			query = "SELECT DISTINCT ?name  WHERE { "
					+ "<" + URI + "> rdf:type " + map(subauthority) + " . "
					+ "<" + URI + "> bf:title ?y . "
					+ "?y rdfs:label ?name . "
					+ "}";
			break;
		}
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

		ResultSet prs = getResultSet(prefix + query);
		while (prs.hasNext()) {
			QuerySolution psol = prs.nextSolution();
			String label = psol.get("?name").asLiteral().getString();
			logger.info("\tlabel: " + label);
			theDocument.add(new TextField("content", retokenizeString(label, true), Field.Store.NO));
		}

		theDocument.add(new StoredField("payload", generateSubauthorityPayload(URI, subauthority)));
		logger.debug("");

		theWriter.addDocument(theDocument);
		if (++count % 100000 == 0)
			logger.info("[" + threadID + "] count: " + count);
	}
}
