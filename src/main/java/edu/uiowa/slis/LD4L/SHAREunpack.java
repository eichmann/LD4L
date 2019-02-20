package edu.uiowa.slis.LD4L;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

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
	for (File file : folder.listFiles()) {
	    if (file.getName().endsWith(".tgz")) {
		processTar(file);
	    }
	}
    }
    
    static void processTar(File tarFile) throws CompressorException, ArchiveException, IOException {
	logger.info("processing tar file: " + tarFile.getName());
	File target = new File(tarFile.getParent()+"/"+tarFile.getName().replace(".tgz", ".nq"));
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
		writer.write(buffer.replace("_{$tagForBlankNode.getAttribute('tag')}", "") + "\n");
		logger.debug("\tquad: " + buffer);
	    }
	    entry = (TarArchiveEntry) tin.getNextEntry();
	}
	is.close();
	writer.close();
    }

}
