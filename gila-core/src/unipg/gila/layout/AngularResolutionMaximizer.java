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
	protected Logger log = Logger.getLogger(this.getClass());

	//	private float k;
	public static final String angleMaximizerModeratorString = "layout.angularModeration";
	public static final float angleMaximizerModeratorDefault = 1.0f;

	private float k;
	private float angleModerator;
	private int currentLayer;
	private boolean clockwiseRotation;
	
	public static final String translationString = "layout.translationModeration";
	public static final float translationModeratorDefault = 1.0f;
	private float translationModerator;

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
		currentLayer = ((IntWritable)getAggregatedValue("AGG_CURRENTLAYER")).get();
		clockwiseRotation = ((BooleanWritable)getAggregatedValue(LayoutRoutine.angleMaximizationClockwiseAggregator)).get();

		translationModerator = getConf().getFloat(translationString, translationModeratorDefault);

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
		HashMap<LayeredPartitionedLongWritable, Float> slopesMap = new HashMap<LayeredPartitionedLongWritable, Float>();

		Iterator<LayoutMessage> iteratorMessages = messages.iterator();
		while(iteratorMessages.hasNext()){
			LayoutMessage lms = iteratorMessages.next();
			float[] value = lms.getValue();
			coordinatesMap.put(lms.getPayloadVertex(), value);
			double computedAtan = Math.atan2(value[1] - myCoordinates[1], value[0] - myCoordinates[0]);
			computedAtan = computedAtan < 0 ? (Math.PI*2 + computedAtan) : computedAtan;
			slopesMap.put(lms.getPayloadVertex(), new Float(computedAtan));
		}

		Map<LayeredPartitionedLongWritable, Float> orderedSlopesMap = Toolbox.sortByValue(slopesMap, clockwiseRotation);

		Iterator<Entry<LayeredPartitionedLongWritable, Float>> slopesIterator = orderedSlopesMap.entrySet().iterator();

		float threshold = new Float(Math.PI*2)/(float)slopesMap.size();//vertex.getNumEdges();
//		log.info("threshold angle " + (threshold*57.298f));

		LayeredPartitionedLongWritable currentVertex = null;
//		LayeredPartitionedLongWritable firstVertex = null;
		
//		log.info("I-m " + vertex.getId());
		if(slopesMap.size() <= 2){
			return;
		}
		log.info(myCoordinates[0] + " " + myCoordinates[1]);
		boolean onemore = false;
		boolean lastNotch = true;
		while(slopesIterator.hasNext() || onemore){
			if(currentVertex == null){
				currentVertex = slopesIterator.next().getKey().copy();
//				log.info("mine " + coordinatesMap.get(currentVertex)[0] + " " + coordinatesMap.get(currentVertex)[1]);
//				firstVertex = currentVertex;
			}
			Entry<LayeredPartitionedLongWritable, Float> nextVertex;
			if(onemore){
				onemore = false;
				nextVertex = orderedSlopesMap.entrySet().iterator().next();
//				log.info("Activating lastone");
			}else
				nextVertex = slopesIterator.next();
//			log.info("current angle " + (slopesMap.get(currentVertex)*57.298f));
//			log.info("next angle " + (slopesMap.get(nextVertex.getKey())*57.298f));
			float currentSlope = slopesMap.get(nextVertex.getKey()) - slopesMap.get(currentVertex);
//			currentSlope = currentSlope > 0 ? currentSlope : new Float(Math.PI*2 + currentSlope);
//			if(currentSlope > threshold){
//				log.info("cur clope " + (currentSlope*57.298f));
				float[] foreignCoordinates = coordinatesMap.get(nextVertex.getKey());
//				log.info("foreigne " + foreignCoordinates[0] + " " + foreignCoordinates[1]);
//				float currentDistance = Toolbox.computeModule(myCoordinates, foreignCoordinates);
				float currentDistance = ((IntWritable)vertex.getEdgeValue(nextVertex.getKey())).get()*k;
				//			float difference = idealDistance - currentDistance;
				//			difference *= angleModerator;
				//			currentDistance += difference;
//				log.info("la distancia " + currentDistance);
				float desiredRotation = (clockwiseRotation ? threshold - currentSlope : -1*(threshold - currentSlope));
//				log.info("desierd rotescion " + desiredRotation*57.298f);
//				log.info("rotescion " + (slopesMap.get(nextVertex.getKey())+desiredRotation)*57.298f);
//				log.info("new coordinates suggested " + (myCoordinates[0] + currentDistance*new Float(Math.cos(slopesMap.get(nextVertex.getKey())+desiredRotation))) + " "
//						+(myCoordinates[1] + currentDistance*new Float(Math.sin(slopesMap.get(nextVertex.getKey())+desiredRotation))));

				float[] suggestedTranslation = new float[]{
						translationModerator*((myCoordinates[0] 
											+ currentDistance*new Float(Math.cos(angleModerator*(slopesMap.get(nextVertex.getKey())+desiredRotation)))) 
											- foreignCoordinates[0]),
						translationModerator*((myCoordinates[1] 
											+ currentDistance*new Float(Math.sin(angleModerator*(slopesMap.get(nextVertex.getKey())+desiredRotation))))
											- foreignCoordinates[1])};
				
//				log.info("suggestori " + suggestedTranslation[0] + " " + suggestedTranslation[1]);
				slopesMap.put(nextVertex.getKey(), new Float((slopesMap.get(nextVertex.getKey()) + desiredRotation)));
				coordinatesMap.put(nextVertex.getKey(), 
						new float[]{foreignCoordinates[0] + suggestedTranslation[0], foreignCoordinates[1] + suggestedTranslation[1]});

				sendMessage(nextVertex.getKey(), new LayoutMessage(nextVertex.getKey(), suggestedTranslation));
//			}
			currentVertex = nextVertex.getKey().copy();
			if(!slopesIterator.hasNext() && lastNotch){
				onemore = true;
				lastNotch = false;
			}
		}
	}

	public static class AverageCoordinateUpdater
	extends AbstractComputation<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, LayoutMessage, LayoutMessage>{


		protected Logger log = Logger.getLogger(this.getClass());

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
				LayoutMessage msg = it.next();
				if(!vertex.getId().equals(msg.getPayloadVertex()))
					continue;
				float[] current = msg.getValue();
				msgsCounter++;
				xAccumulator += current[0];
				yAccumulator += current[1];
			}
			float[] myCoordinates = vertex.getValue().getCoordinates();

			if(msgsCounter > 0){
//				log.info("total messes " + msgsCounter);
//				log.info("finalle " + xAccumulator/(float)msgsCounter + " " + yAccumulator/(float)msgsCounter);

				vertex.getValue().setCoordinates(myCoordinates[0] + xAccumulator/(float)msgsCounter, myCoordinates[1] + yAccumulator/(float)msgsCounter);
			}
		}
	}

}
