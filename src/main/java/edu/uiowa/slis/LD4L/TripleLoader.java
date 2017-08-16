package edu.uiowa.slis.LD4L;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.SQLException;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.util.FileManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TripleLoader implements Runnable {
    static boolean local = false;
    static Logger logger = Logger.getLogger(TripleLoader.class);
    static String pathPrefix = local ? "/Volumes/Spare1/LD4L/" : "/usr/local/RAID/Corpora/LD4L/draft.ld4l.org/downloads/rdf_files/";
    static String fileName = "";
    static PipedRDFIterator<Triple> iter = null;
    static PipedRDFStream<Triple> inputStream = null;
    
    static Reasoner reasoner = null;
//    static PrintStream printstream = null;
    
    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.error");

	Model schema = FileManager.get().loadModel(local ? "file:/Users/eichmann/downloads/ontology.rdf" : "file:/usr/local/RAID/LD4L/ontology.rdf");
	reasoner = ReasonerRegistry.getOWLMicroReasoner();
	reasoner = reasoner.bindSchema(schema);
	
//	printstream = new PrintStream((local ? "/Volumes/Spare2/" : "/usr/local/RAID/LD4L/inference_dumps/") + args[0] + ".nt");

	switch (args[0]) {
	case "cornell":
	    processTarFile(pathPrefix + "cornell/" + "cornell.ld4l.full-catalog.2016-03-17.tar.gz");
	    break;
	case "harvard1":
	    processTarFile(pathPrefix + "harvard/" + "harvard.ld4l.full-catalog-1.2016-03-24.tar.gz");
	    break;
	case "harvard":
	    processTarFile(pathPrefix + "harvard/" + "harvard.ld4l.full-catalog-1.2016-03-24.tar.gz");
	    processTarFile(pathPrefix + "harvard/" + "harvard.ld4l.full-catalog-2.2016-03-22.tar.gz");
	    processTarFile(pathPrefix + "harvard/" + "harvard.ld4l.full-catalog-3.2016-03-21.tar.gz");
	    processTarFile(pathPrefix + "harvard/" + "harvard.ld4l.full-catalog-4.2016-03-22.tar.gz");
	    break;
	case "stanford1":
	    processTarFile(pathPrefix + "stanford/" + "stanford.ld4l.full-catalog-1.2016-03-23.tar.gz");
	    break;
	case "stanford":
	    processTarFile(pathPrefix + "stanford/" + "stanford.ld4l.full-catalog-1.2016-03-23.tar.gz");
	    processTarFile(pathPrefix + "stanford/" + "stanford.ld4l.full-catalog-2.2016-03-22.tar.gz");
	    processTarFile(pathPrefix + "stanford/" + "stanford.ld4l.full-catalog-3.2016-03-21.tar.gz");
	    processTarFile(pathPrefix + "stanford/" + "stanford.ld4l.full-catalog-4.2016-03-22.tar.gz");
	    break;
	default:
	    break;
	}
//	printstream.close();
    }

    static void processTarFile(String path) throws Exception {
	File input = new File(path);
	InputStream is = new FileInputStream(input);
	CompressorInputStream in = new CompressorStreamFactory().createCompressorInputStream("gz", is);

	ArchiveInputStream tin = new ArchiveStreamFactory().createArchiveInputStream("tar", in);
	TarArchiveEntry entry = (TarArchiveEntry) tin.getNextEntry();

	while (entry != null) {
	    if (!entry.isDirectory()) {
		logger.info("\tentry: " + entry.getName());
		logger.info("\t\tmodification time: " + entry.getModTime());
		logger.info("\t\tsize: " + entry.getSize());
		loadFile(entry.getName(), tin);
	    }

	    entry = (TarArchiveEntry) tin.getNextEntry();
	}
    }
    
    static void loadFile(String fileName, InputStream theStream) throws SQLException, IOException {
	int count = 0;
	logger.info("\ttriples: " + fileName);
	TripleLoader.fileName = fileName;
	iter = new PipedRDFIterator<Triple>();
	inputStream = new PipedTriplesStream(iter);
	StringBuffer buffer = new StringBuffer();
	String line = null;
	BufferedReader reader = new BufferedReader(new InputStreamReader(theStream));
	while ((line = reader.readLine()) != null) {
	    buffer.append(line);
	    logger.debug(line);
	}
        Thread thread = new Thread(new TripleLoader(buffer.toString()));
        thread.start();

	Model model = ModelFactory.createDefaultModel();
        // We will consume the input on the main thread here

        // We can now iterate over data as it is parsed, parsing only runs as
        // far ahead of our consumption as the buffer size allows
        while (iter.hasNext()) {
            Triple next = iter.next();
            model.add(model.asStatement(next));
            logger.debug("triple: " + next);
            logger.debug("\tpredicate: " + next.getPredicate());
            logger.debug("\tobject: " + next.getObject().toString(false));

            count++;
        }
	logger.info("\tcount: " + count);
	logger.info("generating inference model...");
	InfModel infmodel = ModelFactory.createInfModel(reasoner, model);
	logger.info("inferred model size: " + infmodel.listStatements().toList().size());
	RDFDataMgr.write(System.out, infmodel, Lang.NTRIPLES);
//	Model diffmodel = infmodel.difference(model);
//	logger.info("iterating over inferred triples...");
//	int infCount = 0;
//	for (Statement stmt : diffmodel.listStatements().toList()) {
//	    logger.info("inferred statement: " + stmt);
//	    infCount++;
//	}
//	logger.info("inferred count: " + infCount);
    }

    InputStream tin = null;
    String buffer = null;
    
    public TripleLoader(String buffer) {
	this.buffer = buffer;
    }
    
    public void run() {
        RDFDataMgr.parse(inputStream, new StringReader(buffer), Lang.NTRIPLES);
    }
  }
