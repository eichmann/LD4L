package edu.uiowa.slis.LD4L.ShortStory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommaByExtractor extends Extractor {
    Pattern pattern = Pattern.compile("(.*) *, *([bB]y *)?([^/]+)$");
    Pattern pattern2 = Pattern.compile("(.*[!?]) *([bB]y *)?([^/]+)$");

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
	    story.setTitle(matcher.group(1));
	    story.setAuthor(new Author(matcher.group(3)));
	} else {
	    Matcher matcher2 = pattern2.matcher(entry);
	    if (matcher2.find()) {
		story.setTitle(matcher2.group(1));
		story.setAuthor(new Author(matcher2.group(3)));
	    } else {
		story.setTitle(entry);
	    }
	}
	
	return story;
    }
}
