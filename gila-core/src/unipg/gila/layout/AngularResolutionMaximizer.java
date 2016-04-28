/**
 * 
 */
package unipg.gila.layout;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.utils.Toolbox;

/**
 * @author Alessio Arleo
 *
 */
public class AngularResolutionMaximizer
extends AbstractComputation<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, LayoutMessage, LayoutMessage>{

	private float k;
	public static final String angleMaximizerModeratorString = "layout.angularModeration";
	public static final float angleMaximizerModeratorDefault = 1.0f;
	private float angleModerator;
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
	 */
	@Override
	public void initialize(
			GraphState graphState,
			WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> workerClientRequestProcessor,
			GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> graphTaskManager,
			WorkerGlobalCommUsage workerGlobalCommUsage,
			WorkerContext workerContext) {
		super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
				workerGlobalCommUsage, workerContext);
		k = ((FloatWritable)getAggregatedValue(LayoutRoutine.k_agg)).get();
		angleModerator = getConf().getFloat(angleMaximizerModeratorString, angleMaximizerModeratorDefault);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
			Iterable<LayoutMessage> messages) throws IOException {
		if(vertex.getNumEdges() < 2)
			return;
		float[] myCoordinates = vertex.getValue().getCoordinates();
		HashMap<LayeredPartitionedLongWritable, Float> distancesMap = new HashMap<LayeredPartitionedLongWritable, Float>();
		Iterator<LayoutMessage> iteratorMessages = messages.iterator();
		while(iteratorMessages.hasNext()){
			LayoutMessage lms = iteratorMessages.next();
			distancesMap.put(lms.getPayloadVertex(), Toolbox.computeModule(myCoordinates, lms.getValue()));
		}
		
		HashMap<LayeredPartitionedLongWritable, Float> slopesMap = Toolbox.buildSlopesMap(messages.iterator(), vertex);
		Map<LayeredPartitionedLongWritable, Float> orderedSlopesMap = Toolbox.sortByValue(slopesMap);
		Iterator<LayeredPartitionedLongWritable> slopesIterator = orderedSlopesMap.keySet().iterator();
		
		

		boolean first = true;
		float firstSlope = 0.0f;
		LayeredPartitionedLongWritable lastVertex = null;
		float lastSlope = 0.0f;;
		while(slopesIterator.hasNext()){
			LayeredPartitionedLongWritable currentVertex = slopesIterator.next();
			float currentSlope = orderedSlopesMap.get(currentVertex);
			if(first){
				firstSlope = currentSlope;
				lastSlope = currentSlope;
				lastVertex = currentVertex.copy();
				first = false;
				continue;
			}
			if(!slopesIterator.hasNext()){
				orderedSlopesMap.put(currentVertex, firstSlope + (new Float(Math.PI*2) - currentSlope));
				continue;
			}
			orderedSlopesMap.put(lastVertex, currentSlope - lastSlope);
			lastSlope = currentSlope;
			lastVertex = currentVertex.copy();
		}
		float threshold = new Float(Math.PI*2)/(float)vertex.getNumEdges();
		Iterator<Entry<LayeredPartitionedLongWritable, Float>> revisedSlopesIterator = orderedSlopesMap.entrySet().iterator();
		while(slopesIterator.hasNext()){
			Entry<LayeredPartitionedLongWritable, Float> currentSlope = revisedSlopesIterator.next();
//			if(currentSlope.getValue() > threshold){
//				float currentDistance = distancesMap.get(currentSlope.getKey());
				float currentDistance = ((IntWritable)vertex.getEdgeValue(currentSlope.getKey())).get()*(float)k;
				sendMessage(currentSlope.getKey(), new LayoutMessage(currentSlope.getKey(), new float[]{
																		myCoordinates[0] + currentDistance*new Float(angleModerator*Math.cos(threshold - currentSlope.getValue())),
																		myCoordinates[1] + currentDistance*new Float(angleModerator*Math.sin(threshold - currentSlope.getValue()))}));
//			}
			
		}
	}
	
	public static class AverageCoordinateUpdater
	extends AbstractComputation<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, LayoutMessage, LayoutMessage>{

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			Iterator<LayoutMessage> it = messages.iterator();
			int msgsCounter = 0;
			float xAccumulator = 0.0f;
			float yAccumulator = 0.0f;
			while(it.hasNext()){
				float[] current = it.next().getValue();
				msgsCounter++;
				xAccumulator += current[0];
				yAccumulator += current[1];
			}
			if(msgsCounter > 0)
				vertex.getValue().setCoordinates(xAccumulator/(float)msgsCounter, yAccumulator/(float)msgsCounter);
		}
	}

}
