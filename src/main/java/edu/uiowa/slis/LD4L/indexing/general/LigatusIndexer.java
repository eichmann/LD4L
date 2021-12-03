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

public class LigatusIndexer extends ThreadedIndexer implements Runnable {

	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure("log4j.info");
		loadProperties("ligatus");

		String query = " SELECT DISTINCT ?uri ?subject where { " + "  ?uri rdf:type skos:Concept . "
				+ "  ?uri skos:prefLabel ?subject . " + "  FILTER (lang(?subject) = 'en') " + "}";
		queue(query);
		instantiateWriter();
		process(MethodHandles.lookup().lookupClass());
		closeWriter();
	}

	int threadID = 0;

	public LigatusIndexer(int threadID) {
		logger.info("LigatusIndexer thread: " + threadID);
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
				indexLoCPerformance(uri);
			} catch (IOException | InterruptedException e) {
				logger.error("Exception raised: " + e);
			}
		}
	}

	void indexLoCPerformance(String URI) throws CorruptIndexException, IOException, InterruptedException {
		String subject = null;
		String query = "SELECT ?uri ?subject WHERE { " + "<" + URI + "> skos:prefLabel ?subject . "
				+ "  FILTER (lang(?subject) = 'en') " + "} ";
		logger.trace("query: " + query);
		ResultSet rs = getResultSet(prefix + query);
		while (rs.hasNext()) {
			QuerySolution sol = rs.nextSolution();
			subject = sol.get("?subject").asLiteral().getString();
		}
		logger.info("\tprefLabel: " + subject);

		Document theDocument = new Document();
		theDocument.add(new StringField("uri", URI, Field.Store.YES));
		theDocument.add(new StringField("name", subject, Field.Store.YES));
		theDocument.add(new StringField("name_lower", subject.toLowerCase(), Field.Store.YES));
		theDocument.add(new TextField("content", retokenizeString(subject, true), Field.Store.NO));

		String query1 = "SELECT DISTINCT ?lab WHERE { " + "<" + URI + "> skos:prefLabel ?lab . " + "}";
		ResultSet ars = getResultSet(prefix + query1);
		while (ars.hasNext()) {
			QuerySolution asol = ars.nextSolution();
			String name = asol.get("?lab").asLiteral().getString();
			logger.info("\t\tprefLabel: " + name);
			theDocument.add(new TextField("content", retokenizeString(name, true), Field.Store.NO));
		}

		String query2 = "SELECT DISTINCT ?lab WHERE { " + "<" + URI + "> skos:altLabel ?lab . " + "}";
		ars = getResultSet(prefix + query2);
		while (ars.hasNext()) {
			QuerySolution asol = ars.nextSolution();
			String name = asol.get("?lab").asLiteral().getString();
			logger.info("\t\taltLabel: " + name);
			theDocument.add(new TextField("content", retokenizeString(name, true), Field.Store.NO));
		}

		theDocument.add(new StoredField("payload", generatePayload(URI)));

		theWriter.addDocument(theDocument);
		count++;
	}

}
