package hdfshelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.streamsx.topology.function.Supplier;

public class HDFSFileIterator implements Iterator<String>,Iterable<String>{

	final static byte NEWLINE=0xA;
	final static byte CARRIAGE_RETURN=13;
	final String filename; 
	final long startOffset;
	final long endOffset;
	boolean finished = false;
	transient BufferedReader reader; 
	transient FSDataInputStream inStream;
	int numLines = 0;
	
	public HDFSFileIterator(String file, long start, long end) {
		Logger.getLogger(HDFSFileIterator.class.getCanonicalName()).log(Level.ALL,"HDFSFileIterator constructor");
		filename = file;
		startOffset = start;
		endOffset = end;
	}
	
	public void remove() {
		
	}
	
	public boolean hasNext() {
	try {	
		if (finished|| (inStream != null &&
				 inStream.getPos() >= endOffset)) {
			return false;
		}
		else return true;
	}
	catch (IOException e) {
		return false;
	}
	}
	
	public String next() {
		if (reader == null && !finished) {
			init();
		}
		if (reader != null) {
			try {
			String line = reader.readLine();
			if (line == null || inStream.getPos() >= endOffset) {
				// File is done, let's cleanup.
				reader.close();
				reader = null;
				finished = true;
				System.out.println(numLines+" read. End offset actually "+inStream.getPos()+" aiming for "+endOffset);
				return "done";
				
			}
			else {
				if (numLines == 0) {
					System.out.println("First line: "+line);
				}
				numLines++;
				return line;
			}
			}
			catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		else {
			System.out.println("Unable to initialize");
			return null;
		}
	}
	
	void init() {
		final Logger logger = Logger.getLogger(HDFSFileIterator.class.getCanonicalName());
		System.out.println("Init");
		Configuration conf = new Configuration();
		System.out.println("Configuration created");
		FileSystem fSystem;
		try {
			fSystem = FileSystem.get(conf);
			if (fSystem == null) {
				System.out.println("Problem getting FileSystem");
			}
			inStream = fSystem.open(new Path(filename));
			if (startOffset > 0) {
				inStream.seek(startOffset-1);
				byte myByte = inStream.readByte();
				while(myByte != NEWLINE && myByte != CARRIAGE_RETURN) {
					myByte = inStream.readByte();
				}
				if (myByte == CARRIAGE_RETURN) {
					if (inStream.readByte() != NEWLINE) {
						logger.log(Level.SEVERE,"Expected new line at "+(inStream.getPos()-1)+" found "+(int)myByte);
					}
				}
			}
			System.out.println("start advanced to "+startOffset);
			if (inStream == null) {
				logger.log(Level.SEVERE,"Problem opening file "+filename);
			}
			reader = new BufferedReader(new InputStreamReader(inStream));
			logger.log(Level.INFO,"File successfully opened.");
		} catch (IOException e) {
			logger.log(Level.SEVERE,"Problem opening file: "+e);
			finished = true;
			inStream = null;
			reader = null;
		}
	}

	@Override
	public Iterator<String> iterator() {
		return this;
	}

}
