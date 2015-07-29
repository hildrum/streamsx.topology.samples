package hdfshelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.streams.operator.PERuntime;
import com.ibm.streams.operator.internal.runtime.api.PEContext;
import com.ibm.streamsx.topology.function.Supplier;

public class HDFSFileReadIterator implements  Iterator<String> {

	final String filename; 
	transient BufferedReader reader; 
	boolean inputDone=false;

	
	public HDFSFileReadIterator(String file) {
		filename = file;
		inputDone=false;
	}
	
	@Override
	public boolean hasNext() {
		return !inputDone;
	}

	@Override
	public String next() {

			try {
				if (reader == null && !inputDone) {
					init();
				}
			String line = reader.readLine();
			if (line == null) {
				// File is done, let's cleanup.
				reader.close();
				reader = null;
				inputDone = true;
				return "done";
			}
			else {
				return line;
			}
			}
			catch (IOException e) {
				inputDone = true;
				e.printStackTrace();
				return null;
			}
	}
	
	void init() {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/opt/ibm/biginsights/hadoop-conf/core-site.xml"));
		
		System.out.println("Configuration created");
		FileSystem fSystem;
		try {
			//Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
			fSystem = FileSystem.get(conf);
			if (fSystem == null) {
				System.out.println("Problem getting FileSystem");
			}
			reader = new BufferedReader( new InputStreamReader (fSystem.open(new Path(filename))));
			System.out.println("Reader successfully initialized");
		} catch (IOException e) {
			System.out.println("Caught IOException ");
			e.printStackTrace();
			reader = null;
		}
	}
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove not supported");
	}
	
	

}