package edu.uiowa.slis.LD4L.indexing;

import java.util.Vector;

public class Queue<QueueInstance> {
    int capacity = 5;
    Vector<QueueInstance> queue = new Vector<QueueInstance>();
    boolean completed = false;
    
    public synchronized boolean atCapacity() {
	return queue.size() >= capacity;
    }
    
    public synchronized void queue(QueueInstance instance) {
	queue.add(instance);
	completed = false;
    }
    
    public void completed() {
	completed = true;
    }
    
    public int size() {
	return queue.size();
    }
    
    public synchronized boolean isCompleted() {
	return queue.size() == 0 && completed;
    }
    
    public synchronized QueueInstance dequeue() {
	if (queue.size() == 0)
	    return null;
	else
	    return queue.remove(0);
    }

}
