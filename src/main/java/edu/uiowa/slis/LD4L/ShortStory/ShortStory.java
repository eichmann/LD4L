package edu.uiowa.slis.LD4L.ShortStory;

import java.util.Vector;

public class ShortStory {
    String title = null;
    Vector<Author> authors = new Vector<Author>();
    
    public ShortStory() {
	
    }
    
    public ShortStory(String title, Author author) {
	this.title = title;
	this.authors.add(author);
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Vector<Author> getAuthors() {
        return authors;
    }

    public void setAuthor(Author author) {
	this.authors.add(author);
    }
    
    public String toString() {
	return authors + " : " + title;
    }
}
