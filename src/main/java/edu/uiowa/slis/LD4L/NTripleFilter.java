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

public class NTripleFilter {
	static Logger logger = Logger.getLogger(NTripleFilter.class);
	static boolean doObjectTriples = true;
	static boolean doLiteralTriples = false;
	static boolean doSubjects = false;
	static boolean doPredicates = false;
	static boolean doObjects = true;

	public static void main(String[] args) throws IOException, InterruptedException {
		PropertyConfigurator.configure(args[0]);
		Pattern objectPat = Pattern.compile("^<([^>]+)> *<([^>]+)> *<([^>]+)> *[.]$");
		Pattern literalPat = Pattern.compile("^<([^>]+)> *<([^>]+)> *(.*) *[.]$");

		BufferedReader reader = new BufferedReader(new InputStreamReader((new FileInputStream(new File(args[1])))),1000000);
		String buffer = null;
		String subject = null;
		String predicate = null;
		String object = null;
		while ((buffer = reader.readLine()) != null) {
			
			if (doObjectTriples) {
				Matcher objectMatch = objectPat.matcher(buffer);
				if (objectMatch.find() && !buffer.contains("> \"<")) {
					logger.debug("object matched: " + buffer);
					if (doSubjects) {
						subject = URIref.encode(StringEscapeUtils.unescapeJava(objectMatch.group(1)));
						logger.debug("\tsubject: " + subject);
					} else
						subject =  objectMatch.group(1);
					if (doPredicates) {
						predicate = URIref.encode(StringEscapeUtils.unescapeJava(objectMatch.group(2)));
						logger.debug("\tpredicate: " + predicate);
					} else
						predicate = objectMatch.group(2);
					if (doObjects) {
						object = URIref.encode(StringEscapeUtils.unescapeJava(objectMatch.group(3)));
						logger.debug("\tobject: " + object);
					} else
						object = objectMatch.group(3);
					System.out.println("<" + subject + "> <" + predicate + "> <" + object + "> .");
					continue;
				} 
			}
			if (doLiteralTriples) {
				Matcher literalMatch = literalPat.matcher(buffer);
				if (literalMatch.find() && !buffer.contains("> \"<")) {
					logger.debug("literal matched: " + buffer);
					if (doSubjects) {
						subject = URIref.encode(StringEscapeUtils.unescapeJava(literalMatch.group(1)));
						logger.debug("\tsubject: " + subject);
					} else
						subject =  literalMatch.group(1);
					if (doPredicates) {
						predicate = URIref.encode(StringEscapeUtils.unescapeJava(literalMatch.group(2)));
						logger.debug("\tpredicate: " + predicate);
					} else
						predicate = literalMatch.group(2);
					String literal = literalMatch.group(3).trim();
					logger.debug("\tliteral: " + literal);
					System.out.println("<" + subject + "> <" + predicate + "> " + literal + " .");
					continue;
				} 
			}
			System.out.println(buffer);
		}
		reader.close();
	}

}
