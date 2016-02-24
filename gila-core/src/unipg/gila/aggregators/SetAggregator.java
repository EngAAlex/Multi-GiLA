package unipg.gila.aggregators;

import java.util.HashSet;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.LongWritable;

public class SetAggregator implements Aggregator<LongWritable> {

	private HashSet<Long> internalState;
	
	@Override
	public void aggregate(LongWritable in) {
		internalState.add(in.get());
	}

	@Override
	public LongWritable createInitialValue() {
		internalState = new HashSet<Long>();
		return new LongWritable(0);
	}

	@Override
	public LongWritable getAggregatedValue() {
		return new LongWritable(internalState.size());
	}

	@Override
	public void reset() {
		internalState.clear();
	}

	@Override
	public void setAggregatedValue(LongWritable initialSeed) {
		createInitialValue();
	}

}
