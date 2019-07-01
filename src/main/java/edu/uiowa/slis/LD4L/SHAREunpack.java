package edu.uiowa.slis.LD4L;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.jena.assembler.exceptions.UnknownEncodingException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RiotException;
import org.apache.jena.sparql.core.Quad;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class SHAREunpack {
	static Logger logger = Logger.getLogger(SHAREunpack.class);

    public static void main(String[] args) throws CompressorException, ArchiveException, IOException {
	PropertyConfigurator.configure(args[0]);
//	File request = new File("/Users/eichmann/downloads/ckb");
	File request = new File("/Volumes/Pegasus3/LD4L/vde/ckb");
	if (request.isDirectory()) {
	    for (File file : request.listFiles()) {
		processFile(file);
	    }
	} else {
	    processFile(request);
	}
    }
    
    static void processFile(File file) throws IOException, CompressorException, ArchiveException {
	logger.info("processing " + file.getName() + "...");
	File target = file.getName().endsWith("tgz") ? new File(file.getParent() + "/" + file.getName().replace(".tgz", ".nq"))
		: new File(file.getParent() + "/" + file.getName().replace(".tar.gz", ".nq"));
	FileWriter writer = new FileWriter(target);
	InputStream is = new FileInputStream(file);
	CompressorInputStream in = new CompressorStreamFactory().createCompressorInputStream("gz", is);

	ArchiveInputStream tin = new ArchiveStreamFactory().createArchiveInputStream("tar", in);
	TarArchiveEntry entry = (TarArchiveEntry) tin.getNextEntry();
	while (entry != null) {
	    BufferedReader reader = new BufferedReader(new InputStreamReader((InputStream) tin));
	    String buffer = null;
	    Model model = ModelFactory.createDefaultModel();
	    while ((buffer = reader.readLine()) != null) {
		logger.debug("\tquad: " + buffer);
		try {
		    RDFDataMgr.read(model,new ByteArrayInputStream(buffer.getBytes()), RDFFormat.NQUADS.getLang());
		    writer.write(buffer + "\n");
		} catch (RiotException e) {
		    logger.error("entry: " + entry.getName());
		    logger.error("error parsing: " + buffer);
		}
	    }
	    entry = (TarArchiveEntry) tin.getNextEntry();
	}
	is.close();
	writer.close();
    }
	
     static void rewriteMain(String[] args) throws CompressorException, ArchiveException, IOException {
	PropertyConfigurator.configure(args[0]);
	File folder = new File("/Volumes/Pegasus3/LD4L/vde/cornell");
//	File folder = new File("/Users/eichmann/downloads/cornell");
//	for (File file : folder.listFiles()) {
//	    if (file.getName().endsWith(".tgz")) {
//		processTar(file);
//	    }
//	}
	processTar(new File("/Volumes/Pegasus3/LD4L/vde/SVDE_cornell_Phase3_D1_20190605_01_v2.tar.gz"));
    }
    
    static void processTar(File tarFile) throws CompressorException, ArchiveException, IOException {
	logger.info("processing tar file: " + tarFile.getName());
	File target = tarFile.getName().endsWith("tgz")
			? new File(tarFile.getParent()+"/"+tarFile.getName().replace(".tgz", ".nq"))
			: new File(tarFile.getParent()+"/"+tarFile.getName().replace(".tar.gz", ".nq"));
//	File target = new File("/Volumes/SSD2/cornell/"+tarFile.getName().replace(".tgz", ".nq"));
	FileWriter writer = new FileWriter(target);
	InputStream is = new FileInputStream(tarFile);
	CompressorInputStream in = new CompressorStreamFactory().createCompressorInputStream("gz", is);

	ArchiveInputStream tin = new ArchiveStreamFactory().createArchiveInputStream("tar", in);
	TarArchiveEntry entry = (TarArchiveEntry) tin.getNextEntry();

	while (entry != null) {
	    logger.debug("entry: " + entry.getName());
	    BufferedReader reader = new BufferedReader(new InputStreamReader((InputStream)tin));
	    String buffer = null;
	    while ((buffer = reader.readLine()) != null) {
		writer.write(rewrite(buffer) + "\n");
		logger.debug("\tquad: " + buffer);
	    }
	    entry = (TarArchiveEntry) tin.getNextEntry();
	}
	is.close();
	writer.close();
    }
    
    static Pattern blankPattern = Pattern.compile("^(.*vocabulary/languages/[^ >]*) +([^>]*)(>.*)$");
    static Pattern quotePattern = Pattern.compile("^(<[^>]+> *<[^>]+> *)\"\"?([^\"]+)\"\"( +<.*)$");
    static Pattern bracketPattern = Pattern.compile("^(.*)<([^>]+)>> +<(.*)$");
    static Pattern slashPattern = Pattern.compile("^(<[^>]+> *<[^>]+> *)\"([^\\\"]+[^\\\\])\\\\\"( +<.*)$");
    static Pattern blankPattern2 = Pattern.compile("^(.*vocabulary/organizations/[^ >]*) +([^>]*)(>.*)$");
    static Pattern bracePattern = Pattern.compile("^(.*)/\\{(.*)\\}(.*)$");
    
    static String rewrite(String buffer) {
	Matcher matcher = null;
//	if (buffer.contains("tagForBlank"))
//	    return buffer.replace("_{$tagForBlankNode.getAttribute('tag')}", "");
//	matcher = blankPattern.matcher(buffer);
//	if (matcher.matches()) {
//	    logger.info("buffer:" + buffer);
//	    logger.info("\tmatch 1: " + matcher.group(1));
//	    logger.info("\tmatch 2: " + matcher.group(2));
//	    logger.info("\tmatch 3: " + matcher.group(3));
//	    if (matcher.group(2).length() == 0)
//		return matcher.group(1) + matcher.group(3);
//	    else
//		return matcher.group(1) + "_" + matcher.group(2).trim().replace(" ", "_") + matcher.group(3);
//	}
//	matcher = blankPattern2.matcher(buffer);
//	if (matcher.matches()) {
//	    logger.info("buffer:" + buffer);
//	    logger.info("\tmatch 1: " + matcher.group(1));
//	    logger.info("\tmatch 2: " + matcher.group(2));
//	    logger.info("\tmatch 3: " + matcher.group(3));
//	    if (matcher.group(2).length() == 0)
//		return matcher.group(1) + matcher.group(3);
//	    else
//		return matcher.group(1) + "_" + matcher.group(2).trim().replace(" ", "_") + matcher.group(3);
//	}
//	matcher = quotePattern.matcher(buffer);
//	if (matcher.matches()) {
//	    logger.info("quote buffer:" + buffer);
//	    String literal = matcher.group(2);
//	    if (literal.endsWith("\\"))
//		literal = literal.substring(0, literal.length()-1);
//	    logger.info("\tmatch 1: " + matcher.group(1));
//	    logger.info("\tmatch 2: " + literal);
//	    logger.info("\tmatch 3: " + matcher.group(3));
//	    return matcher.group(1) + "\"" + literal + "\"" + matcher.group(3);
//	}
//	matcher = bracketPattern.matcher(buffer);
//	if (matcher.matches()) {
//	    logger.info("buffer:" + buffer);
//	    logger.info("\tmatch 1: " + matcher.group(1));
//	    logger.info("\tmatch 2: " + matcher.group(2));
//	    logger.info("\tmatch 3: " + matcher.group(3));
//	    return matcher.group(1) + matcher.group(2) + "> <" + matcher.group(3);
//	}
//	matcher = slashPattern.matcher(buffer);
//	if (matcher.matches()) {
//	    logger.info("buffer:" + buffer);
//	    logger.info("\tmatch 1: " + matcher.group(1));
//	    logger.info("\tmatch 2: " + matcher.group(2));
//	    logger.info("\tmatch 3: " + matcher.group(3));
//	    return matcher.group(1) + "\"" + matcher.group(2) + "\"" + matcher.group(3);
//	}
	matcher = bracePattern.matcher(buffer);
	if (matcher.matches()) {
	    logger.info("buffer:" + buffer);
	    logger.info("\tmatch 1: " + matcher.group(1));
	    logger.info("\tmatch 2: " + matcher.group(2));
	    logger.info("\tmatch 3: " + matcher.group(3));
	    return matcher.group(1) + "/_" + matcher.group(2) + "_" + matcher.group(3);
	}
	buffer = buffer.replace("|||>", "___>");
	buffer = buffer.replace("|>", "_>");
	String buffer2 = buffer.replace("\";%20target=\"%5Fblank", "");
	if (!buffer2.equals(buffer))
	    logger.info("buffer:" + buffer);
	return buffer2;
    }

}
