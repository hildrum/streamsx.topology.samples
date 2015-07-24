package app;

import hdfshelper.HDFSFileIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;

public class ReadHDFSInParallel {

	public static long BLOCK_SIZE = 67108864;
	public static class FileSplit {
		public final String filename;
		public final long startOffset;
		public final long endOffset;
		
		public FileSplit(String file, long start, long end) {
			filename = file;
			startOffset = start;
			endOffset = end;
		}
		
		public String toString() {
			return filename+"["+startOffset+","+endOffset+")";
		}
		
	}
	/**
	 * @param args
	 * @throws Exception 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException, Exception {
		
		Topology flow = new Topology();
		
		TStream<String> filenames = flow.strings("netro_20130826083601.log");
		
		TStream<FileSplit> splits = filenames.multiTransform(new Function<String,Iterable<FileSplit> >() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			FileSystem fSystem = null;
			
			private void init() throws IOException {
				Configuration conf = new Configuration();
				conf.addResource(new Path("/opt/ibm/biginsights/hadoop-conf/core-site.xml"));
				fSystem = FileSystem.get(conf);
			}
			
			@Override
			public Iterable<FileSplit> apply(String v) {

			FileStatus stats;
			try {
				if (fSystem == null) {
					init();
				}
				stats = fSystem.getFileStatus(new Path(v));

			List<FileSplit> toReturn = new ArrayList<FileSplit>((int)(stats.getLen()/BLOCK_SIZE));
			for (long start = 0; start <= stats.getLen(); start+= BLOCK_SIZE) {
				toReturn.add(new FileSplit(v,start,start+BLOCK_SIZE));
			}
			return toReturn;
			} catch (IllegalArgumentException | IOException e) {
				return null;
			}
		}
		}
		, FileSplit.class);
		splits.print();
		TStream<FileSplit> parallelizedSplits = splits.parallel(2);
		parallelizedSplits.print();
		TStream<String> lines = parallelizedSplits.multiTransform(new Function<FileSplit,Iterable<String>>() {
			@Override
			public Iterable<String> apply(FileSplit v) {
				return new HDFSFileIterator(v.filename,v.startOffset,v.endOffset);
			}
		}, String.class);
	
		TStream<SummaryStats> stats = lines.transform(new CountByType(),SummaryStats.class);
		
		TStream<SummaryStats> statsMerged = stats.unparallel();
		
		statsMerged.print();
		flow.addJarDependency("/opt/ibm/biginsights/IHC/share/hadoop/common/lib/commons-configuration-1.6.jar");
		flow.addJarDependency("/opt/ibm/biginsights/IHC/share/hadoop/common/lib/commons-cli-1.2.jar");
		flow.addJarDependency("/opt/ibm/biginsights/IHC/hadoop-core.jar");
		flow.addJarDependency("/opt/ibm/biginsights/IHC/share/hadoop/common/lib/commons-logging-1.1.1.jar");
		StreamsContextFactory.getStreamsContext("STANDALONE").submit(flow).get();
	

	}

}
