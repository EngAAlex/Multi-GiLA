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
package unipg.gila.multi.coarseners;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

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
import unipg.gila.common.datastructures.messagetypes.SingleLayerLayoutMessage;
import unipg.gila.common.datastructures.messagetypes.LayoutMessageMatrix;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.common.multi.SolarMessage;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.partitioning.Spinner;

public class InterLayerCommunicationUtils{

	public static final String destroyLevelsString = "placer.destroyLevels";
	
	//LOGGER
	protected static Logger log = Logger.getLogger(InterLayerCommunicationUtils.class);

	
	/**
	 * This computation broadcasts the vertex coordinates before transferring them to the underlying layer at the next superstep.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class CoordinatesBroadcast extends MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage>{

		/* (non-Javadoc)
		 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			if(vertex.getValue().getLowerLevelWeight() > 0)
				sendMessageToAllEdges(vertex, new LayoutMessage(vertex.getId(), vertex.getValue().getCoordinates()));
		}
	}

	/**
	 * This computation transfers the data about the upper level vertices to the lower level ones.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class InterLayerDataTransferComputation extends
	MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage> {

		private boolean destroyLevels;
		
		/* (non-Javadoc)
		 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			AstralBodyCoordinateWritable value = vertex.getValue();
			if(value.getLowerLevelWeight() > 0){
				LayeredPartitionedLongWritable mineId = vertex.getId();
				LayeredPartitionedLongWritable lowerID = new LayeredPartitionedLongWritable(mineId.getPartition(), mineId.getId(), mineId.getLayer() - 1);
				sendMessage(lowerID, new LayoutMessage(lowerID, value.getCoordinates()));
				Iterator<LayoutMessage> it = msgs.iterator();
				while(it.hasNext())
					sendMessage(lowerID, (LayoutMessage) it.next().propagateAndDie());
				removeEdgesRequest(lowerID, vertex.getId());
			}
			if(destroyLevels){
				removeVertexRequest(vertex.getId());
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
			destroyLevels = getConf().getBoolean(destroyLevelsString, true);
		}
	}


	public static class MergerToPlacerDummyComputation extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, LayoutMessageMatrix<LayeredPartitionedLongWritable>>{

		Random rnd;
		float bBoxX;
		float bBoxY;

		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<SolarMessage> msgs) throws IOException {
				float selectedX = rnd.nextFloat()*bBoxX;
				float selectedY = rnd.nextFloat()*bBoxY;
				vertex.getValue().setCoordinates(selectedX, selectedY);
			return;
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
			rnd = new Random();
			bBoxX = getConf().getFloat(Spinner.bBoxStringX, 1200.0f);
			bBoxY = getConf().getFloat(Spinner.bBoxStringY, bBoxX);
		}
	}
}
