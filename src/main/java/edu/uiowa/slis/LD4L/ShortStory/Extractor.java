package edu.uiowa.slis.LD4L.ShortStory;

public class Extractor {

    // , Jr.
    // Jr.
    // , III
    // III
    // ... [et al.]
    // et al.
    // ... et al.
    // , Esq.
    // , Ph.D
    // . [n more not listed]
    
    static String tailJunk = "(?:((\\.\\.\\.)? \\[et al\\.\\])|(\\. *\\[ *[0-9]+ more not listed *\\]))?";
//    static String tailJunk = "(\\. *\\[ *[0-9]+ more not listed *\\])?";
    
    public int matchCount(String[] entries) {
	int matches = 0;
	
	return matches;
    }

    public ShortStory extract(String entry) {
	return null;
    }
}