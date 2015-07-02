package systemthelpers;

import java.util.Map;
import java.util.List;
import com.ibm.avatar.algebra.datamodel.FieldSetter;
import com.ibm.avatar.algebra.datamodel.Text;
import com.ibm.avatar.algebra.datamodel.TupleList;
import com.ibm.avatar.algebra.datamodel.TupleSchema;

import com.ibm.avatar.algebra.util.tokenize.TokenizerConfig;
import com.ibm.avatar.api.ExternalTypeInfo;
import com.ibm.avatar.api.ExternalTypeInfoImpl;
import com.ibm.avatar.api.OperatorGraph;
import com.ibm.avatar.api.exceptions.TextAnalyticsException;
import com.ibm.streamsx.topology.function7.Function;

/**
 * A bare-bones text analytics operator graph executor. 
 * @author hildrum
 *
 */
public class GraphRunner implements Function<String,Map>{

	transient OperatorGraph analyzer;
	transient TupleSchema systemTInputSchema;
	transient FieldSetter<String> textSetter;
	transient String outputViews[];
	final String moduleNames[];
	final String modulePath;
	
	public GraphRunner(String moduleNames[],String modulePath) throws Exception{
		this.moduleNames = moduleNames;
		this.modulePath = modulePath;
		init(moduleNames,modulePath);
	}
	
	private Object readResolve() throws Exception {
		init(moduleNames,modulePath);
		return this;
	}
	
	
	public void init(String moduleNames[], String modulePath) throws Exception{
		ExternalTypeInfo info = new ExternalTypeInfoImpl();
		// This is where you'd add external dictionaries and external tables.F
		analyzer = OperatorGraph.createOG(moduleNames, modulePath, info, new TokenizerConfig.Standard());
		systemTInputSchema = analyzer.getDocumentSchema();
		textSetter = systemTInputSchema.textSetter("text");
		outputViews = analyzer.getOutputTypeNames().toArray(new String[0]);
	}
	
	
	@Override
	public Map<String,TupleList> apply(String inputDoc) {
		com.ibm.avatar.algebra.datamodel.Tuple tupleForSysT = systemTInputSchema.createTup();
		textSetter.setVal(tupleForSysT,inputDoc);
		
		try {
			Map<String,TupleList> toReturn = analyzer.execute(tupleForSysT,outputViews,null);
			return toReturn;
		}
		catch (TextAnalyticsException e) {
			System.err.println(e);
			return null;
		}
	}

}
