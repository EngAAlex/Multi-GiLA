/**
 * 
 */
package unipg.gila.multi;

import java.io.IOException;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import unipg.gila.multi.common.LayeredPartitionedLongWritable;

public abstract class MultiScaleComputation<Z extends Writable, P extends Writable, T extends Writable> extends
		AbstractComputation<LayeredPartitionedLongWritable, Z, FloatWritable, P, T> {
		
	protected int currentLayer;
	
	@Override	
	public void compute(
			Vertex<LayeredPartitionedLongWritable, Z, FloatWritable> vertex,
			Iterable<P> msgs) throws IOException {
		if(vertex.getId().getLayer() != currentLayer)
			return;
		else{
			vertexInLayerComputation(vertex, msgs);
		}
	}

	@Override
	public void initialize(
			GraphState graphState,
			WorkerClientRequestProcessor<LayeredPartitionedLongWritable, Z, FloatWritable> workerClientRequestProcessor,
			GraphTaskManager<LayeredPartitionedLongWritable, Z, FloatWritable> graphTaskManager,
			WorkerGlobalCommUsage workerGlobalCommUsage,
			WorkerContext workerContext) {
		super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
				workerGlobalCommUsage, workerContext);
		currentLayer = ((IntWritable)getAggregatedValue(MultiScaleDirector.currentLayer)).get();
	}
	
	protected abstract void vertexInLayerComputation(Vertex<LayeredPartitionedLongWritable, Z, FloatWritable> vertex,
			Iterable<P> msgs) throws IOException;
	
}

