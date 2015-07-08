package testapp;

import java.util.concurrent.ExecutionException;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.function7.Function;

public class TwoStreams {

	public static void main(String argv[]) throws InterruptedException, ExecutionException, Exception {
		
		Topology flow = new Topology("TestCopy");

		TStream<Tracker> trackers = flow.limitedSourceN(new Function<Long,Tracker>() {

		@Override
		public Tracker apply(Long v) {
			return new Tracker(v);
		}
		}, 10, Tracker.class);

		TStream<Tracker> alice = trackers.transform(new Function<Tracker,Tracker>() {

			@Override
			public Tracker apply(Tracker v) {
				v.touch("alice");
				return v;
			}
			
		}
		,Tracker.class);
		
	TStream<Tracker> bob = trackers.transform(new Function<Tracker,Tracker>() {

		@Override
		public Tracker apply(Tracker v) {
			v.touch("bob");
			return v;
		}	
	}
	,Tracker.class);
	
	alice.print();
	bob.print();
	StreamsContextFactory.getStreamsContext("EMBEDDED").submit(flow).get();

	}
}
