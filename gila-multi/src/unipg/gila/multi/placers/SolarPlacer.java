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
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.SpTreeEdgeValue;
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

	protected double k;
	protected boolean clearInfo;

	/* (non-Javadoc)
	 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	protected void vertexInLayerComputation(
			Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
			Iterable<LayoutMessage> msgs) throws IOException {
		AstralBodyCoordinateWritable value = vertex.getValue();

		if(!value.isSun())// && value.getLowerLevelWeight() == 0)
			return;
		
    if(SolarPlacerRoutine.logPlacer)
      log.info("I'm " + vertex.getId());

		Iterator<LayoutMessage> itmsgs = msgs.iterator();
		HashMap<LayeredPartitionedLongWritable, double[]> coordsMap = new HashMap<LayeredPartitionedLongWritable, double[]>();
		HashMap<LayeredPartitionedLongWritable, double[]> planetsComputedCoordsMap = new HashMap<LayeredPartitionedLongWritable, double[]>();
		HashMap<LayeredPartitionedLongWritable, double[]> moonsComputedCoordsMap = new HashMap<LayeredPartitionedLongWritable, double[]>();

		while(itmsgs.hasNext()){ //Check all messages and gather all the coordinates (including updating mine).
			LayoutMessage msg = itmsgs.next();
			if(msg.getPayloadVertex().equals(vertex.getId())){
				value.setCoordinates(msg.getValue()[0], msg.getValue()[1]);
			}else{
				if(SolarPlacerRoutine.logPlacer)
					log.info("Analyzing message from upper level " + msg.getPayloadVertex());
				coordsMap.put(msg.getPayloadVertex(), msg.getValue());
			}
		}

		double[] myCoords = vertex.getValue().getCoordinates();
		if(SolarPlacerRoutine.logPlacer)
			log.info("Arranging bodies");
		//ARRANGE PLANETS
		if(value.planetsNo() > 0){
			if(SolarPlacerRoutine.logPlacer)
				log.info("Arranging planets");

			Iterator<Entry<Writable, Writable>> planetsIterator = vertex.getValue().getPlanetsIterator();
			arrangeBodies(value.getCoordinates(), planetsIterator, planetsComputedCoordsMap, coordsMap, value);


			//SEND ALL PACKETS TO PLANETS
			Iterator<Entry<LayeredPartitionedLongWritable, double[]>> finalIteratorOnPlanets = planetsComputedCoordsMap.entrySet().iterator();
			while(finalIteratorOnPlanets.hasNext()){
				Entry<LayeredPartitionedLongWritable, double[]> currentRecipient = finalIteratorOnPlanets.next();
				double[] chosenPosition = currentRecipient.getValue();
				if(chosenPosition != null)
					sendMessage(currentRecipient.getKey().copy(), new LayoutMessage(currentRecipient.getKey().copy(), new double[]{myCoords[0] + chosenPosition[0],
						myCoords[1] + chosenPosition[1]}));
				else{
					double angle = Math.random()*Math.PI*2;
					double desiredDistance = vertex.getEdgeValue(currentRecipient.getKey()).getValue()*k;
					sendMessage(currentRecipient.getKey().copy(), new LayoutMessage(currentRecipient.getKey(), 
							new double[]{myCoords[0] + Math.cos(angle)*desiredDistance,
						myCoords[1] + Math.sin(angle)*desiredDistance}));
				}
	       addEdgeRequest(vertex.getId(), EdgeFactory.create(currentRecipient.getKey(), new SpTreeEdgeValue(true))); 
	       addEdgeRequest(currentRecipient.getKey(), EdgeFactory.create(vertex.getId(), new SpTreeEdgeValue(true))); 
			}

			//ARRANGE MOONS
			if(value.moonsNo() > 0){
				if(SolarPlacerRoutine.logPlacer)
					log.info("Arranging moons");
				Iterator<Entry<Writable, Writable>> moonsIterator = vertex.getValue().getMoonsIterator();
				if(moonsIterator != null)
					arrangeBodies(value.getCoordinates(), moonsIterator, moonsComputedCoordsMap, coordsMap, value);		

				//SEND ALL PACKETS TO MOONS
				Iterator<Entry<LayeredPartitionedLongWritable, double[]>> finalIteratorOnMoons = moonsComputedCoordsMap.entrySet().iterator();
				while(finalIteratorOnMoons.hasNext()){
					Entry<LayeredPartitionedLongWritable, double[]> currentRecipient = finalIteratorOnMoons.next();
					double[] chosenPosition = currentRecipient.getValue();
		       if(SolarPlacerRoutine.logPlacer)
		         log.info("Sending placing information to " + currentRecipient.getKey());
					if(chosenPosition != null)
					  sendMessageToAllEdges(vertex, new LayoutMessage(currentRecipient.getKey().copy(), 1, 
					    new double[]{myCoords[0] + chosenPosition[0], myCoords[1] + chosenPosition[1]}));
					else{
						LayoutMessage messageToSend = new LayoutMessage(currentRecipient.getKey().copy(), 1,
								new double[]{-1.0f, -1.0f});
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
	private void arrangeBodies(double[] myCoordinates,
			Iterator<Entry<Writable, Writable>> bodiesIterator,
			HashMap<LayeredPartitionedLongWritable, double[]> bodiesMap, HashMap<LayeredPartitionedLongWritable, double[]> coordsMap, AstralBodyCoordinateWritable allNeighbors) throws IOException{
		if(SolarPlacerRoutine.logPlacer)
			log.info("Checking sets");
		while(bodiesIterator.hasNext()){
			float avgX = 0.0f, avgY = 0.0f;
			Entry<Writable, Writable> current = bodiesIterator.next();
			PathWritableSet setOfPaths = (PathWritableSet) current.getValue();
			if(SolarPlacerRoutine.logPlacer)
				log.info("My planet/moon " + ((LayeredPartitionedLongWritable)current.getKey()).getId());
			if(setOfPaths.size() == 0){
				bodiesMap.put((LayeredPartitionedLongWritable) current.getKey(), null);
				if(SolarPlacerRoutine.logPlacer)
					log.info("vertex was not found on any path ");
				continue;
			}
			Iterator<PathWritable> deSetIterator = (Iterator<PathWritable>) setOfPaths.iterator();
			while(deSetIterator.hasNext()){
				PathWritable currentPath = deSetIterator.next();

				LayeredPartitionedLongWritable translatedId = 
						new LayeredPartitionedLongWritable(currentPath.getReferencedSun().getPartition(), 
								currentPath.getReferencedSun().getId(), currentLayer + 1);
				if(SolarPlacerRoutine.logPlacer){
					log.info("Analyzing " + currentPath.getReferencedSun() + " thru " + translatedId);
					log.info("path towards " + currentPath.getReferencedSun() +
							" at position " + currentPath.getPositionInpath() + 
							" on a total of " + allNeighbors.getPathLengthForNeighbor(currentPath.getReferencedSun()));
				}

				double deltaX = coordsMap.get(translatedId)[0] - myCoordinates[0];
				double deltaY = coordsMap.get(translatedId)[1] - myCoordinates[1];

				avgX += deltaX*(currentPath.getPositionInpath()/(float)allNeighbors.getPathLengthForNeighbor(currentPath.getReferencedSun()));
				avgY += deltaY*(currentPath.getPositionInpath()/(float)allNeighbors.getPathLengthForNeighbor(currentPath.getReferencedSun()));

			}
			if(SolarPlacerRoutine.logPlacer){
				log.info("Number of paths for vertex: "  + setOfPaths.size());
				log.info("computed position: " + avgX/setOfPaths.size() + " " + avgY/setOfPaths.size());
			}
			float randomness = Math.random() > 0.5 ? new Float(Math.random()) : new Float(-Math.random());
			bodiesMap.put((LayeredPartitionedLongWritable) current.getKey(), new double[]{(avgX/setOfPaths.size()) + randomness, avgY/setOfPaths.size() + randomness});
		}		
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.MultiScaleComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
	 */
	@Override
	public void initialize(
			GraphState graphState,
			WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> workerClientRequestProcessor,
			GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> graphTaskManager,
			WorkerGlobalCommUsage workerGlobalCommUsage,
			WorkerContext workerContext) {
		super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
				workerGlobalCommUsage, workerContext);
		k = ((DoubleWritable)getAggregatedValue(LayoutRoutine.k_agg)).get();
		clearInfo = getConf().getBoolean(InterLayerCommunicationUtils.destroyLevelsString, true);
	}

}
