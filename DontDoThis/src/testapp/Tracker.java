package testapp;

import java.util.ArrayList;
import java.util.List;

public class Tracker {

	long num; 
	private List<String> touchedBy;
	
	public Tracker(long n) {
		num = n;
		touchedBy = new ArrayList<String>();
	}
	
	synchronized public void touch(String toucherName) {
		touchedBy.add(toucherName);
	}
	
	public String toString() {
		StringBuilder myString = new StringBuilder("");
		myString.append(num);
		myString.append(": [");
		for(String l : touchedBy) {
			myString.append(l);
			myString.append(" ");
		}
		myString.append("]");
		return myString.toString();
	}
}
