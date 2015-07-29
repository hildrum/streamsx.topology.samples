package hdfshelper;

import java.util.Iterator;

public class HDFSFileSplitReadIterable implements Iterable<String> {

	final String filename;
	final long start;
	final long end;
	public HDFSFileSplitReadIterable(String filename, long start, long end) {
		this.filename = filename;
		this.start = start;
		this.end = end;
	}
	@Override
	public Iterator<String> iterator() {
		return new HDFSFileSplitReadIterator(filename,start,end);
	}


}
