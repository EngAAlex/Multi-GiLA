/**
 * 
 */
package unipg.gila.multi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.common.datastructures.messagetypes.MessageWritable;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;

public abstract class MultiScaleComputation<Z extends Writable, P extends MessageWritable, T extends MessageWritable> extends
AbstractComputation<LayeredPartitionedLongWritable, Z, IntWritable, P, T> {

	//LOGGER
	Logger log = Logger.getLogger(MultiScaleComputation.class);

	protected int currentLayer;

	@Override	
	public void compute(
			Vertex<LayeredPartitionedLongWritable, Z, IntWritable> vertex,
			Iterable<P> msgs) throws IOException {
		if(vertex.getId().getLayer() != currentLayer)
			return;
		else{
			log.info("I'm " + vertex.getId());
			vertexInLayerComputation(vertex, msgs);
		}
	}

	@Override
	public void initialize(
			GraphState graphState,
			WorkerClientRequestProcessor<LayeredPartitionedLongWritable, Z, IntWritable> workerClientRequestProcessor,
			GraphTaskManager<LayeredPartitionedLongWritable, Z, IntWritable> graphTaskManager,
			WorkerGlobalCommUsage workerGlobalCommUsage,
			WorkerContext workerContext) {
		super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
				workerGlobalCommUsage, workerContext);
		currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get();
	}

	protected abstract void vertexInLayerComputation(Vertex<LayeredPartitionedLongWritable, Z, IntWritable> vertex,
			Iterable<P> msgs) throws IOException;

	/* (non-Javadoc)
	 * @see org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable#getConf()
	 */
	@SuppressWarnings("unchecked")
	public ImmutableClassesGiraphConfiguration<LayeredPartitionedLongWritable, Writable, IntWritable> getSpecialConf() {
		// TODO Auto-generated method stub
		return (ImmutableClassesGiraphConfiguration<LayeredPartitionedLongWritable, Writable, IntWritable>) super.getConf();
	}

	public void sendMessageWithWeight(Vertex<LayeredPartitionedLongWritable, Z, IntWritable> vertex,
						LayeredPartitionedLongWritable id, T msg){
//		MessageWritable w = (MessageWritable) msg;
		msg.addToWeight(((IntWritable)vertex.getEdgeValue(id)).get());
		log.info("sendind " + msg);
		sendMessage(id, msg);
	}

	/**
	 * 
	 */
	public void sendMessageToMultipleEdgesWithWeight(Vertex<LayeredPartitionedLongWritable, Z, IntWritable> vertex, Iterator<LayeredPartitionedLongWritable> vertexIdIterator, T message) {
		while(vertexIdIterator.hasNext()){
			MessageWritable messageCopy = message.copy();
			sendMessageWithWeight(vertex, vertexIdIterator.next(), (T)messageCopy);
		}
	}
	
	/**
	 * 
	 */
	public void sendMessageToAllEdgesWithWeight(
			Vertex<LayeredPartitionedLongWritable, Z, IntWritable> vertex,
			T message) {
		Iterator<Edge<LayeredPartitionedLongWritable, IntWritable>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){			
			LayeredPartitionedLongWritable current = edges.next().getTargetVertexId();
			if(current.getLayer() == currentLayer)
				sendMessageWithWeight(vertex, current, (T) message.copy());
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#sendMessageToAllEdges(org.apache.giraph.graph.Vertex, org.apache.hadoop.io.Writable)
	 */
	@Override
	public void sendMessageToAllEdges(
			Vertex<LayeredPartitionedLongWritable, Z, IntWritable> vertex,
			T message) {
		Iterator<Edge<LayeredPartitionedLongWritable, IntWritable>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			LayeredPartitionedLongWritable current = edges.next().getTargetVertexId();
			if(current.getLayer() == currentLayer)
				sendMessage(current, message);
		}
	}

}

