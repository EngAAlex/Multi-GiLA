/**
 * 
 */
package unipg.gila.layout;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.utils.Toolbox;

/**
 * @author Alessio Arleo
 *
 */
public class AngularResolutionMaximizer
extends AbstractComputation<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, LayoutMessage, LayoutMessage>{

	//LOGGER
	Logger log = Logger.getLogger(this.getClass());

	private float k;
	public static final String angleMaximizerModeratorString = "layout.angularModeration";
	public static final float angleMaximizerModeratorDefault = 1.0f;
	public static final String translationString = "layout.translationModeration";
	public static final float translationModeratorDefault = 1.0f;

	private float translationModerator;
	private float angleModerator;
	private int currentLayer;
	private boolean clockwiseRotation;

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
		translationModerator = getConf().getFloat(translationString, translationModeratorDefault);
		currentLayer = ((IntWritable)getAggregatedValue("AGG_CURRENTLAYER")).get();
		clockwiseRotation = ((BooleanWritable)getAggregatedValue(LayoutRoutine.angleMaximizationClockwiseAggregator)).get();
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
			Iterable<LayoutMessage> messages) throws IOException {
		if(vertex.getId().getLayer() != currentLayer || vertex.getNumEdges() < 2)
			return;
		float[] myCoordinates = vertex.getValue().getCoordinates();
		HashMap<LayeredPartitionedLongWritable, float[]> coordinatesMap = new HashMap<LayeredPartitionedLongWritable, float[]>();
		Iterator<LayoutMessage> iteratorMessages = messages.iterator();
		while(iteratorMessages.hasNext()){
			LayoutMessage lms = iteratorMessages.next();
			//			distancesMap.put(lms.getPayloadVertex(), Toolbox.computeModule(myCoordinates, lms.getValue()));
			coordinatesMap.put(lms.getPayloadVertex(), lms.getValue());
		}

		HashMap<LayeredPartitionedLongWritable, Float> slopesMap = Toolbox.buildSlopesMap(messages.iterator(), vertex);
		Map<LayeredPartitionedLongWritable, Float> orderedSlopesMap = Toolbox.sortByValue(slopesMap);
		Iterator<LayeredPartitionedLongWritable> slopesIterator = orderedSlopesMap.keySet().iterator();



		boolean first = true;
		float firstSlope = 0.0f;
		LayeredPartitionedLongWritable lastVertex = null;
		float lastSlope = 0.0f;
		log.info("I'm " + vertex.getId());
		while(slopesIterator.hasNext()){
			LayeredPartitionedLongWritable currentVertex = slopesIterator.next();
			float currentSlope = orderedSlopesMap.get(currentVertex);
			log.info("Current slope " + currentSlope + " for vertex " + currentVertex);
			if(first){
				firstSlope = currentSlope;
				lastSlope = currentSlope;
				lastVertex = currentVertex.copy();
				first = false;
				continue;
			}
			if(!slopesIterator.hasNext()){
				orderedSlopesMap.put(currentVertex, new Float(firstSlope + (Math.PI*2 - currentSlope)));
				continue;
			}
			orderedSlopesMap.put(lastVertex, currentSlope - lastSlope);
			lastSlope = currentSlope;
			lastVertex = currentVertex.copy();
		}
		float threshold = new Float(Math.PI*2)/(float)vertex.getNumEdges();
		log.info("Current threshold " + threshold);
		Iterator<Entry<LayeredPartitionedLongWritable, Float>> revisedSlopesIterator = orderedSlopesMap.entrySet().iterator();
		while(revisedSlopesIterator.hasNext()){
			Entry<LayeredPartitionedLongWritable, Float> currentSlope = revisedSlopesIterator.next();
			log.info("suggested correction from " + currentSlope.getValue() + " " + (threshold - currentSlope.getValue()));
			//			if(currentSlope.getValue() > threshold){
			//				float currentDistance = (((IntWritable)vertex.getEdgeValue(currentSlope.getKey())).get()*(float)k) - distancesMap.get(currentSlope.getKey());
			float[] foreignCoordinates = coordinatesMap.get(currentSlope.getKey());
//			float currentDistance = Toolbox.computeModule(myCoordinates, foreignCoordinates);
//			float deltaX = foreignCoordinates[0] - myCoordinates[0];
//			float deltaY = foreignCoordinates[1] - myCoordinates[1];
			float idealDistance = ((IntWritable)vertex.getEdgeValue(currentSlope.getKey())).get()*(float)k;
//			float completeModeration = translationModerator*(idealDistance - currentDistance);
//			float[] proposedCorrection = new float[]{completeModeration*(deltaX/currentDistance),
//					completeModeration*(deltaY/currentDistance)};
			float desiredRotation = clockwiseRotation ? threshold - currentSlope.getValue() : -1*(threshold - currentSlope.getValue());
			sendMessage(currentSlope.getKey(), new LayoutMessage(currentSlope.getKey(), new float[]{
				translationModerator*((myCoordinates[0] + idealDistance*new Float(Math.cos(angleModerator*(desiredRotation)))) - foreignCoordinates[0]),
				translationModerator*((myCoordinates[1] + idealDistance*new Float(Math.sin(angleModerator*(desiredRotation)))) - foreignCoordinates[1])}));
			//			}
	}
	}

	public static class AverageCoordinateUpdater
	extends AbstractComputation<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, LayoutMessage, LayoutMessage>{


		private int currentLayer;

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
			currentLayer = ((IntWritable)getAggregatedValue("AGG_CURRENTLAYER")).get();
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
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
						float[] myCoordinates = vertex.getValue().getCoordinates();

			if(msgsCounter > 0)
				vertex.getValue().setCoordinates(myCoordinates[0] + xAccumulator/(float)msgsCounter, myCoordinates[1] + yAccumulator/(float)msgsCounter);
		}
	}

}
