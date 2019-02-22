package edu.uiowa.slis.LD4L;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class SHAREunpack {
	static Logger logger = Logger.getLogger(SHAREunpack.class);

    public static void main(String[] args) throws CompressorException, ArchiveException, IOException {
	PropertyConfigurator.configure(args[0]);
	File folder = new File("/Volumes/Pegasus3/LD4L/vde/cornell");
//	File folder = new File("/Users/eichmann/downloads/cornell");
//	for (File file : folder.listFiles()) {
//	    if (file.getName().endsWith(".tgz")) {
//		processTar(file);
//	    }
//	}
	processTar(new File("/Volumes/Pegasus3/LD4L/vde/cornell/Cornell_03.tgz"));
    }
    
    static void processTar(File tarFile) throws CompressorException, ArchiveException, IOException {
	logger.info("processing tar file: " + tarFile.getName());
	File target = new File(tarFile.getParent()+"/"+tarFile.getName().replace(".tgz", ".nq"));
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
    
    static String rewrite(String buffer) {
	if (buffer.contains("tagForBlank"))
	    return buffer.replace("_{$tagForBlankNode.getAttribute('tag')}", "");
	Matcher matcher = blankPattern.matcher(buffer);
	if (matcher.matches()) {
	    logger.debug("buffer:" + buffer);
	    logger.debug("\tmatch 1: " + matcher.group(1));
	    logger.debug("\tmatch 2: " + matcher.group(2));
	    logger.debug("\tmatch 3: " + matcher.group(3));
	    if (matcher.group(2).length() == 0)
		return matcher.group(1) + matcher.group(3);
	    else
		return matcher.group(1) + "_" + matcher.group(2).trim().replace(" ", "_") + matcher.group(3);
	}
	matcher = quotePattern.matcher(buffer);
	if (matcher.matches()) {
	    logger.info("quote buffer:" + buffer);
	    String literal = matcher.group(2);
	    if (literal.endsWith("\\"))
		literal = literal.substring(0, literal.length()-1);
	    logger.info("\tmatch 1: " + matcher.group(1));
	    logger.info("\tmatch 2: " + literal);
	    logger.info("\tmatch 3: " + matcher.group(3));
	    return matcher.group(1) + "\"" + literal + "\"" + matcher.group(3);
	}
	matcher = bracketPattern.matcher(buffer);
	if (matcher.matches()) {
	    logger.info("buffer:" + buffer);
	    logger.info("\tmatch 1: " + matcher.group(1));
	    logger.info("\tmatch 2: " + matcher.group(2));
	    logger.info("\tmatch 3: " + matcher.group(3));
	    return matcher.group(1) + matcher.group(2) + "> <" + matcher.group(3);
	}
	matcher = slashPattern.matcher(buffer);
	if (matcher.matches()) {
	    logger.info("buffer:" + buffer);
	    logger.info("\tmatch 1: " + matcher.group(1));
	    logger.info("\tmatch 2: " + matcher.group(2));
	    logger.info("\tmatch 3: " + matcher.group(3));
	    return matcher.group(1) + "\"" + matcher.group(2) + "\"" + matcher.group(3);
	}
	buffer = buffer.replace("\\043", "");
	return buffer;
    }

}
