package hdfshelper;

import java.util.Iterator;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;

public class HDFSFileReader implements Function<String, Iterable<String>>, Supplier<Iterable<String>> {

	private final String filename;
	private static final long serialVersionUID = 1L;

	public HDFSFileReader(String filename) {
		this.filename = filename;
	}
	
	public HDFSFileReader() {
		this.filename = null;
	}
	
	@Override
	public Iterable<String> get() {
		return new HDFSFileIterable(filename);
	}

	@Override
	public Iterable<String> apply(String filename) {
		return new HDFSFileIterable(filename);
	}



}
