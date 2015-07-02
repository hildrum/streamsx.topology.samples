package demoapp;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import systemthelpers.GraphRunner;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.file.FileStreams;
import com.ibm.avatar.algebra.datamodel.TupleList;
import com.ibm.avatar.algebra.util.tokenize.TokenizerConfig;
import com.ibm.avatar.api.CompilationSummary;
import com.ibm.avatar.api.CompileAQL;
import com.ibm.avatar.api.CompileAQLParams;
import com.ibm.avatar.aql.compiler.CompilerWarning;
import com.ibm.streamsx.topology.function7.Function;
import com.ibm.streamsx.topology.function7.Predicate;

public class ProcessLines {

	public static void compileAQL(String inputModule, String modulePath,
			String outputURI) throws Exception {
		CompileAQLParams params = new CompileAQLParams();
		params.setInputModules(new String[] { inputModule });
		params.setModulePath(modulePath);
		params.setTokenizerConfig(new TokenizerConfig.Standard());
		params.setOutputURI(outputURI);
		CompilationSummary summary = CompileAQL.compile(params);
		for (CompilerWarning warn : summary.getCompilerWarning()) {
			System.err.println("TextAnalytics compiler warning: "
					+ warn.toString());
		}
	}

	public static void main(String argv[]) throws Exception {
		String filename = "PrideAndPrejudice.txt";
		String fullPath = (new File(filename)).getAbsolutePath();
		//String module = "myextractor";
		//String outputview = "DoubleNegative";

		final String module = System.getenv("STREAMS_INSTALL")+"/samples/com.ibm.streams.text/FeatureDemo/etc/getNames";
		final String outputview = "getNames.FullNameWithTitle";
		File outputDir = new File("compiledModules");
		if (!outputDir.exists()) {
			outputDir.mkdirs();
		}

		// Let's compile the AQL.
		compileAQL(module, null, outputDir.toURI().toString());
		// Establish the topology
		Topology flow = new Topology("ProcessLines");
		TStream<String> filenamestream = flow.strings(fullPath);
		TStream<String> lines = FileStreams.textFileReader(filenamestream);
		/*
		 * // Apply the extractor TStream<Map> systemT = lines.transform(new
		 * Function<String,Map>() { public Map<String,TupleList> apply(String
		 * line ) { return new HashMap<String,TupleList>(); } } , Map.class);
		 */
		// Apply the extractor
		TStream<Map> systemT = lines.transform(new GraphRunner(
				new String[] { "getNames" }, outputDir.getAbsolutePath()),
				Map.class);
		// Let's filter out empty lines.
		TStream<Map> justNonEmpty = systemT.filter(new Predicate<Map>() {
			public boolean test(Map inMap) {
				if (inMap.containsKey(outputview)) {
					TupleList tupList = (TupleList) inMap
							.get(outputview);
					return tupList.size() > 0;
				} else
					return false;
			}
		});

		justNonEmpty.print();

		String textToolkit = System.getenv("STREAMS_INSTALL")
				+ "/toolkits/com.ibm.streams.text";
		flow.addJarDependency(textToolkit + "/lib/TextAnalytics/lib/JSON4J.jar");
		flow.addJarDependency(textToolkit
				+ "/lib/TextAnalytics/lib/text-analytics/systemT.jar");
		StreamsContextFactory.getStreamsContext("STANDALONE").submit(flow);
	}

}
