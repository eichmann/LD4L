package edu.uiowa.slis.LD4L.ShortStory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class SlashExtractor extends Extractor {
    static Logger logger = Logger.getLogger(SlashExtractor.class);
    Pattern pattern = Pattern.compile("(.*) */ *([bB]y *)?(([^,&\\[](?!and ))+(, Jr\\.)?)((,| and|&) (.*))?"+tailJunk+"$");

    public int matchCount(String[] entries) {
	int matches = 0;
	
	for (String entry : entries) {
	    Matcher matcher = pattern.matcher(entry);
	    if (matcher.matches())
		matches++;
	}
	
	return matches;
    }
    
    public ShortStory extract(String entry) {
	ShortStory story = new ShortStory();
	
	Matcher matcher = pattern.matcher(entry);
	if (matcher.find()) {
//	    for (int i = 1; i <= matcher.groupCount(); i++) {
//		logger.info("\t\t\tgroup " + i + ": " + matcher.group(i));
//	    }
	    story.setTitle(matcher.group(1));
	    story.setAuthor(new Author(matcher.group(3)));
	    if (matcher.group(7) != null)
		story.setAuthor(new Author(matcher.group(7)));
	} else {
	    story.setTitle(entry);
	}
	
	return story;
    }
}
