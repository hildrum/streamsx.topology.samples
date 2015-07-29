package hdfshelper;

import java.util.Iterator;

public class HDFSFileIterable implements Iterable<String> {

	final String filename;
	public HDFSFileIterable(String file) {
		filename  = file;
	}
	@Override
	public Iterator<String> iterator() {
		return new HDFSFileReadIterator(filename);
	}

}
