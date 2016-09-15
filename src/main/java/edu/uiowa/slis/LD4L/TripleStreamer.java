package edu.uiowa.slis.LD4L;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TripleStreamer {
    static Logger logger = Logger.getLogger(TripleLoader.class);
    static String pathPrefix = "/Volumes/Spare1/LD4L/";
    static String fileName = "";

    public static void main(String[] args) throws Exception {
	PropertyConfigurator.configure("log4j.info");
	switch (args[0]) {
	case "cornell":
	    processTarFile(pathPrefix + "cornell/" + "cornell.ld4l.full-catalog.2016-03-17.tar.gz");
	    break;
	case "harvard":
	    processTarFile(pathPrefix + "harvard/" + "harvard.ld4l.full-catalog-1.2016-03-24.tar.gz");
	    processTarFile(pathPrefix + "harvard/" + "harvard.ld4l.full-catalog-2.2016-03-22.tar.gz");
	    processTarFile(pathPrefix + "harvard/" + "harvard.ld4l.full-catalog-3.2016-03-21.tar.gz");
	    processTarFile(pathPrefix + "harvard/" + "harvard.ld4l.full-catalog-4.2016-03-22.tar.gz");
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
    }

    static void processTarFile(String path) throws Exception {
	File input = new File(path);
	InputStream is = new FileInputStream(input);
	CompressorInputStream in = new CompressorStreamFactory().createCompressorInputStream("gz", is);

	ArchiveInputStream tin = new ArchiveStreamFactory().createArchiveInputStream("tar", in);
	TarArchiveEntry entry = (TarArchiveEntry) tin.getNextEntry();

	while (entry != null) {
	    if (!entry.isDirectory()) {
		logger.debug("\tentry: " + entry.getName());
		logger.debug("\t\tmodification time: " + entry.getModTime());
		logger.debug("\t\tsize: " + entry.getSize());
		loadFile(entry.getName(), tin);
	    }

	    entry = (TarArchiveEntry) tin.getNextEntry();
	}
    }
    
    static void loadFile(String fileName, InputStream theStream) throws SQLException, IOException {
	logger.debug("\ttriples: " + fileName);
	TripleLoader.fileName = fileName;
	String line = null;
	BufferedReader reader = new BufferedReader(new InputStreamReader(theStream));
	while ((line = reader.readLine()) != null) {
	    System.out.println(line);
	}
    }
  }
