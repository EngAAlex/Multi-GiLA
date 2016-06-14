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
package unipg.gila.multi.placers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.common.multi.PathWritable;
import unipg.gila.common.multi.PathWritableSet;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils;

/**
 * @author Alessio Arleo
 *
 */
public class SolarPlacer extends MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage> {

	//LOGGER
	protected Logger log = Logger.getLogger(SolarPlacer.class);

	protected float k;
	protected boolean clearInfo;

	/* (non-Javadoc)
	 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	protected void vertexInLayerComputation(
			Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
			Iterable<LayoutMessage> msgs) throws IOException {
		AstralBodyCoordinateWritable value = vertex.getValue();

		if(!value.isSun())// && value.getLowerLevelWeight() == 0)
			return;

		//		log.info("These are my neighbors:");
		//		log.info(value.neighborSystemsStateToString());
		//		log.info("Upper layer messages received:");

		//		Iterator<Edge<LayeredPartitionedLongWritable, FloatWritable>> edges = vertex.getEdges().iterator();
		//		while(edges.hasNext()){
		//			Edge<LayeredPartitionedLongWritable, FloatWritable> next = edges.next();
		//			if(next.getTargetVertexId().getLayer() == currentLayer +1)
		//				log.info(next.getTargetVertexId().getId());
		//		}

		Iterator<LayoutMessage> itmsgs = msgs.iterator();
		HashMap<LayeredPartitionedLongWritable, float[]> coordsMap = new HashMap<LayeredPartitionedLongWritable, float[]>();
		HashMap<LayeredPartitionedLongWritable, float[]> planetsComputedCoordsMap = new HashMap<LayeredPartitionedLongWritable, float[]>();
		HashMap<LayeredPartitionedLongWritable, float[]> moonsComputedCoordsMap = new HashMap<LayeredPartitionedLongWritable, float[]>();

		while(itmsgs.hasNext()){ //Check all messages and gather all the coordinates (including updating mine).
			LayoutMessage msg = itmsgs.next();
			if(msg.getPayloadVertex().equals(vertex.getId())){
				value.setCoordinates(msg.getValue()[0], msg.getValue()[1]);
			}else{
				if(SolarPlacerRoutine.logPlacer)
					log.info(msg.getPayloadVertex());
				coordsMap.put(msg.getPayloadVertex(), msg.getValue());
			}
		}

		float[] myCoords = vertex.getValue().getCoordinates();
		if(SolarPlacerRoutine.logPlacer)
			log.info("Arranging bodies");
		//ARRANGE PLANETS
		if(value.planetsNo() > 0){
			if(SolarPlacerRoutine.logPlacer)
				log.info("Arranging planets");

			Iterator<Entry<Writable, Writable>> planetsIterator = vertex.getValue().getPlanetsIterator();
			arrangeBodies(value.getCoordinates(), planetsIterator, planetsComputedCoordsMap, coordsMap, value);


			//SEND ALL PACKETS TO PLANETS
			Iterator<Entry<LayeredPartitionedLongWritable, float[]>> finalIteratorOnPlanets = planetsComputedCoordsMap.entrySet().iterator();
			while(finalIteratorOnPlanets.hasNext()){
				Entry<LayeredPartitionedLongWritable, float[]> currentRecipient = finalIteratorOnPlanets.next();
				float[] chosenPosition = currentRecipient.getValue();
				if(chosenPosition != null)
					sendMessage(currentRecipient.getKey().copy(), new LayoutMessage(currentRecipient.getKey().copy(), new float[]{myCoords[0] + chosenPosition[0],
						myCoords[1] + chosenPosition[1]}));
				else{
					float angle = new Float(Math.random()*Math.PI*2);
					float desiredDistance = ((IntWritable)vertex.getEdgeValue(currentRecipient.getKey())).get()*k;
					sendMessage(currentRecipient.getKey().copy(), new LayoutMessage(currentRecipient.getKey(), 
							new float[]{myCoords[0] + new Float(Math.cos(angle)*desiredDistance),
						myCoords[1] + new Float(Math.sin(angle)*desiredDistance)}));
				}
			}

			//ARRANGE MOONS
			if(value.moonsNo() > 0){
				if(SolarPlacerRoutine.logPlacer)
					log.info("Arranging moons");
				Iterator<Entry<Writable, Writable>> moonsIterator = vertex.getValue().getMoonsIterator();
				if(moonsIterator != null)
					arrangeBodies(value.getCoordinates(), moonsIterator, moonsComputedCoordsMap, coordsMap, value);		

				//SEND ALL PACKETS TO MOONS
				Iterator<Entry<LayeredPartitionedLongWritable, float[]>> finalIteratorOnMoons = moonsComputedCoordsMap.entrySet().iterator();
				while(finalIteratorOnMoons.hasNext()){
					Entry<LayeredPartitionedLongWritable, float[]> currentRecipient = finalIteratorOnMoons.next();
					float[] chosenPosition = currentRecipient.getValue();
					if(chosenPosition != null)
						sendMessage(currentRecipient.getKey().copy(), new LayoutMessage(currentRecipient.getKey().copy(), 1, new float[]{myCoords[0] + chosenPosition[0],
							myCoords[1] + chosenPosition[1]}));
					else{
						LayoutMessage messageToSend = new LayoutMessage(currentRecipient.getKey().copy(), 1,
								new float[]{-1.0f, -1.0f});
						messageToSend.setWeight(-1);
						sendMessageToAllEdges(vertex, messageToSend);
					}
				}
			}
		}
		if(clearInfo)
			value.clearAstralInfo();
	}

	//	}

	/**
	 * @param bodiesIterator
	 * @param bodiesMap
	 */
	@SuppressWarnings("unchecked")
	private void arrangeBodies(float[] myCoordinates,
			Iterator<Entry<Writable, Writable>> bodiesIterator,
			HashMap<LayeredPartitionedLongWritable, float[]> bodiesMap, HashMap<LayeredPartitionedLongWritable, float[]> coordsMap, AstralBodyCoordinateWritable allNeighbors) throws IOException{
		if(SolarPlacerRoutine.logPlacer)
			log.info("Checking sets");
		while(bodiesIterator.hasNext()){
			float avgX = 0.0f, avgY = 0.0f;
			Entry<Writable, Writable> current = bodiesIterator.next();
			PathWritableSet deSet = (PathWritableSet) current.getValue();
			if(SolarPlacerRoutine.logPlacer)
				log.info("My planet/moon " + ((LayeredPartitionedLongWritable)current.getKey()).getId());
			if(deSet.size() == 0){
				bodiesMap.put((LayeredPartitionedLongWritable) current.getKey(), null);
				if(SolarPlacerRoutine.logPlacer)
					log.info("vertex was not found on any path ");
				continue;
			}
			Iterator<PathWritable> deSetIterator = (Iterator<PathWritable>) deSet.iterator();
			while(deSetIterator.hasNext()){
				PathWritable currentPath = deSetIterator.next();

				LayeredPartitionedLongWritable translatedId = 
						new LayeredPartitionedLongWritable(currentPath.getReferencedSun().getPartition(), 
								currentPath.getReferencedSun().getId(), currentLayer + 1);
				if(SolarPlacerRoutine.logPlacer){
					log.info("Analyzing + " + currentPath.getReferencedSun() + " thru " + translatedId);
					log.info("path towards " + currentPath.getReferencedSun() +
							" at position " + currentPath.getPositionInpath() + " on a total of " + allNeighbors.getPathLengthForNeighbor(currentPath.getReferencedSun()));
				}

				float deltaX = coordsMap.get(translatedId)[0] - myCoordinates[0];
				float deltaY = coordsMap.get(translatedId)[1] - myCoordinates[1];

				avgX += deltaX*(currentPath.getPositionInpath()/(float)allNeighbors.getPathLengthForNeighbor(currentPath.getReferencedSun()));
				avgY += deltaY*(currentPath.getPositionInpath()/(float)allNeighbors.getPathLengthForNeighbor(currentPath.getReferencedSun()));

			}
			if(SolarPlacerRoutine.logPlacer){
				log.info("Total deset size "  + deSet.size());
				log.info("computed position: " + avgX/deSet.size() + " " + avgY/deSet.size());
			}
			float randomness = Math.random() > 0.5 ? new Float(Math.random()) : new Float(-Math.random());
			bodiesMap.put((LayeredPartitionedLongWritable) current.getKey(), new float[]{(avgX/deSet.size()) + randomness, avgY/deSet.size() + randomness});
		}		
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.MultiScaleComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
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
		clearInfo = getConf().getBoolean(InterLayerCommunicationUtils.destroyLevelsString, true);
	}

}
