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
import java.util.Iterator;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.SpTreeEdgeValue;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils;

/**
 * @author Alessio Arleo
 *
 */
public class PlacerCoordinateDelivery extends MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage> {

	//LOGGER
	Logger log = Logger.getLogger(PlacerCoordinateDelivery.class);

	protected double k;
	protected boolean clearInfo;

	/* (non-Javadoc)
	 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	protected void vertexInLayerComputation(
			Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
			Iterable<LayoutMessage> msgs) throws IOException {
		boolean found = false;
		Iterator<LayoutMessage> ms = msgs.iterator();
		if(!ms.hasNext() || vertex.getValue().isSun())
			return;
    if(SolarPlacerRoutine.logPlacer)
      log.info("I'm " + vertex.getId());
		while(ms.hasNext() && !found){
			LayoutMessage current = (LayoutMessage) ms.next();
			if(current.getPayloadVertex().equals(vertex.getId())){
				vertex.getValue().setCoordinates(current.getValue()[0], current.getValue()[1]);
				found = true;
				if(SolarPlacerRoutine.logPlacer)
					log.info("Received my new coordinates! " + current.toString());
			}
		}
		double[] myCoords = vertex.getValue().getCoordinates();
		ms = msgs.iterator();
		while(ms.hasNext()){
			LayoutMessage current = ms.next();
			if(!current.isAZombie() && !current.getPayloadVertex().equals(vertex.getId())){
			  if(vertex.getEdgeValue(current.getPayloadVertex()) != null){
					if(SolarPlacerRoutine.logPlacer){
						log.info("I'm "+ vertex.getId() + " propagating to my neighbor " + current.getPayloadVertex());
					}
					if(current.getWeight() >= 0){
						if(SolarPlacerRoutine.logPlacer)
							log.info("Retransmitting coordinates");
						sendMessage(current.getPayloadVertex().copy(), (LayoutMessage)current.propagateAndDie());
					}else{
						double angle = Math.random()*Math.PI*2;
						double desiredDistance = vertex.getEdgeValue(current.getPayloadVertex()).getValue()*k;
						double[] blanks = new double[]{myCoords[0] + Math.cos(angle)*desiredDistance,
								myCoords[1] + Math.sin(angle)*desiredDistance};
						if(SolarPlacerRoutine.logPlacer)
							log.info("Blanks received; generating random coordinates");
						sendMessage(current.getPayloadVertex().copy(), new LayoutMessage(current.getPayloadVertex().copy(), blanks));
					}
	        addEdgeRequest(vertex.getId(), EdgeFactory.create(current.getPayloadVertex(), new SpTreeEdgeValue(true))); 
	        addEdgeRequest(current.getPayloadVertex(), EdgeFactory.create(vertex.getId(), new SpTreeEdgeValue(true))); 
				}
			}
		}
		if(clearInfo)
			vertex.getValue().clearAstralInfo();
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
