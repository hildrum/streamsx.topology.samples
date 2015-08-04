package app;

import java.util.Iterator;

import hdfshelper.HDFSFileReadIterator;
import hdfshelper.HDFSFileReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function.Function;



public class ReadAndCount {

	public static void main(String argv[]) throws Exception{
		String filename = "inputTwitterMessages.txt";
		Topology flow = new Topology();
		
	
		TStream<String> files  = flow.strings(filename);
				
		TStream<String> lines =files.multiTransform(new HDFSFileReader(),String.class);
		
		lines.print();
		
		flow.addJarDependency("/opt/ibm/biginsights/IHC/share/hadoop/common/lib/commons-configuration-1.6.jar");
		flow.addJarDependency("/opt/ibm/biginsights/IHC/share/hadoop/common/lib/commons-cli-1.2.jar");
		flow.addJarDependency("/opt/ibm/biginsights/IHC/hadoop-core.jar");
		flow.addJarDependency("/opt/ibm/biginsights/IHC/share/hadoop/common/lib/commons-logging-1.1.1.jar");
		//StreamsContextFactory.getEmbedded().submit(flow).get();
		StreamsContextFactory.getStreamsContext("STANDALONE").submit(flow).get();
	}
		
}
	
