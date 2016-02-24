package unipg.gila.aggregators;

import java.util.HashSet;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.LongWritable;

public class SetAggregator implements Aggregator<LongWritable> {

	private HashSet<Long> internalState;
	
	public void aggregate(LongWritable in) {
		internalState.add(in.get());
	}

	public LongWritable createInitialValue() {
		internalState = new HashSet<Long>();
		return new LongWritable(0);
	}

	public LongWritable getAggregatedValue() {
		return new LongWritable(internalState.size());
	}

	public void reset() {
		internalState.clear();
	}

	public void setAggregatedValue(LongWritable initialSeed) {
		createInitialValue();
	}

}
