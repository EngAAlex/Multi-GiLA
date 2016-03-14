/*******************************************************************************
 * Copyright 2016 Alessio Arleo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
