package edu.uiowa.slis.LD4L;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.jena.util.URIref;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class NTripleEncoder {
    static Logger logger = Logger.getLogger(NTripleEncoder.class);

    public static void main(String[] args) throws IOException {
	PropertyConfigurator.configure(args[0]);
	Pattern objectPat = Pattern.compile("^<([^>]+)> *<([^>]+)> *<([^>]+)> *[.]$");
	Pattern literalPat = Pattern.compile("^<([^>]+)> *<([^>]+)> *(.*) *[.]$");
	
	BufferedReader reader = new BufferedReader(new InputStreamReader((new FileInputStream(new File(args[1])))));
	String buffer = null;
	while ((buffer = reader.readLine()) != null) {
	    Matcher objectMatch = objectPat.matcher(buffer);
	    if (objectMatch.find() && !buffer.contains("> \"<")) {
		logger.debug("object matched: " + buffer);
		String subject = URIref.encode(StringEscapeUtils.unescapeJava(objectMatch.group(1)));
		String predicate = URIref.encode(StringEscapeUtils.unescapeJava(objectMatch.group(2)));
		String object = URIref.encode(StringEscapeUtils.unescapeJava(objectMatch.group(3)));
		logger.debug("\tsubject: " + subject);
		logger.debug("\tpredicate: " + predicate);
		logger.debug("\tobject: " + object);
		System.out.println("<" + subject + "> <" + predicate + "> <" + object + "> .");
		continue;
	    }
	    
	    Matcher literalMatch = literalPat.matcher(buffer);
	    if (literalMatch.find() && !buffer.contains("> \"<")) {
		logger.debug("literal matched: " + buffer);
		String subject = URIref.encode(StringEscapeUtils.unescapeJava(literalMatch.group(1)));
		String predicate = URIref.encode(StringEscapeUtils.unescapeJava(literalMatch.group(2)));
		String literal = literalMatch.group(3).trim();
		logger.debug("\tsubject: " + subject);
		logger.debug("\tpredicate: " + predicate);
		logger.debug("\tliteral: " + literal);
		System.out.println("<" + subject + "> <" + predicate + "> " + literal + " .");
		continue;
	    }
	    
	    System.out.println(buffer);
	}
	reader.close();
    }

}
