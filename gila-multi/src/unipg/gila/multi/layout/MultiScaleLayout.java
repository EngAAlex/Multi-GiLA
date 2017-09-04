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
package unipg.gila.multi.layout;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.SpTreeEdgeValue;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.layout.AbstractPropagator;
import unipg.gila.layout.AbstractSeeder;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorerWithComponentsNo;
import unipg.gila.layout.LayoutRoutine.DrawingScaler;
import unipg.gila.layout.LayoutRoutine.LayoutCCs;
import unipg.gila.multi.MultiScaleMaster;
import unipg.gila.multi.coarseners.SolarMergerRoutine;

/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleLayout {

	protected static Logger log = Logger.getLogger(MultiScaleLayout.class);

	protected static int currentLayer;	

	public static class Seeder extends AbstractSeeder<AstralBodyCoordinateWritable, SpTreeEdgeValue>{

		private double k;

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractSeeder#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
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
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayerAggregator)).get();
			k = ((DoubleWritable)getAggregatedValue(LayoutRoutine.k_agg)).get();
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractSeeder#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			else{
				if(new Float(vertex.getValue().getCoordinates()[0]).isNaN() || new Float(vertex.getValue().getCoordinates()[1]).isNaN())
					throw new IOException("NAN detected");
				super.compute(vertex, messages);
			}
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#sendMessageToAllEdges(org.apache.giraph.graph.Vertex, org.apache.hadoop.io.Writable)
		 */
		@Override
		public void sendMessageToAllEdges(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				LayoutMessage message) {
			int counter = 0;
			Iterator<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> edges = vertex.getEdges().iterator();
			while(edges.hasNext()){
				Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue> currentEdge = edges.next();
				LayeredPartitionedLongWritable current = currentEdge.getTargetVertexId();
				if(current.getLayer() != currentLayer || currentEdge.getValue().isSpanningTree())
					continue;
				aggregate(LayoutRoutine.max_K_agg, new DoubleWritable(vertex.getEdgeValue(current).getValue()*k));
				LayoutMessage msgCopy = ((LayoutMessage)message).copy();
				msgCopy.setSenderId(vertex.getId().getId());
				sendMessage(current, msgCopy);
				counter++;
			}
			getContext().getCounter("Messages Statistics", "Messages sent during drawing process").increment(counter);
		}
	}

	public static class Propagator extends AbstractPropagator<AstralBodyCoordinateWritable, SpTreeEdgeValue>{

		boolean surpassedThreshold;
		double modifier;
		double maxK = Double.MIN_VALUE;

		private HashSet<LayoutMessage> messageCache;

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
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
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayerAggregator)).get();
			modifier = getConf().getDouble(LayoutRoutine.walshawModifierString, LayoutRoutine.walshawModifierDefault);
			maxK = ((DoubleWritable)getAggregatedValue(LayoutRoutine.max_K_agg)).get();
			surpassedThreshold = ((BooleanWritable)getAggregatedValue(MultiScaleMaster.thresholdSurpassedAggregator)).get();
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			else{
				if(new Float(vertex.getValue().getCoordinates()[0]).isNaN() || new Float(vertex.getValue().getCoordinates()[1]).isNaN())
					throw new IOException("NAN detected");
				super.compute(vertex, messages);
			}
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#requestOptimalEdgeLength(org.apache.giraph.graph.Vertex, unipg.gila.common.datastructures.PartitionedLongWritable)
		 */
		@Override
		protected double requestOptimalSpringLength(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SpTreeEdgeValue> eValues) {
			Iterator<SpTreeEdgeValue> cc = eValues.iterator();
			int value = 1;
			while(cc.hasNext()){
				SpTreeEdgeValue current = cc.next();
				if(!current.isSpanningTree()){
					value = current.getValue();
					break;
				}
			}

			if(LayoutRoutine.logLayout)
				log.info("Suggesting a spring length of " + value*k + " based on " + value + " and " + k);
			return value*k;
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#requestWalshawConstant()
		 */
		@Override
		protected double requestWalshawConstant() {
			if(LayoutRoutine.logLayout)
				log.info("Suggested walshawConstant " + Math.pow(maxK,2)*modifier + " from " + Math.pow(maxK,2) + " " + modifier);;
				return Math.pow(maxK,2)*modifier;
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#sendMessageToAllEdges(org.apache.giraph.graph.Vertex, org.apache.hadoop.io.Writable)
		 */
		@Override
		public void sendMessageToAllEdges(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				LayoutMessage message) {
			if(messageCache == null)
				messageCache = new HashSet<LayoutMessage>();
			messageCache.add(message.copy());
		}

		/**
		 * @param message
		 */
		public void sendMessageToAll(Set<LayoutMessage> messages, Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				boolean thresholdSurpassed){
			int counter = 0;
			Iterator<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> onEdges = vertex.getEdges().iterator();
			while(onEdges.hasNext()){
				Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue> currentSpTreeEdge = onEdges.next();
				if(currentSpTreeEdge.getValue().isSpanningTree() == thresholdSurpassed)
					continue;
				Iterator<LayoutMessage> onMessages = messages.iterator();
				while(onMessages.hasNext()){
					LayoutMessage currentMessage = onMessages.next();
					if(currentSpTreeEdge.getTargetVertexId().getId() != currentMessage.getSenderId()){
						currentMessage.setSenderId(vertex.getId().getId());
						sendMessage(currentSpTreeEdge.getTargetVertexId(), currentMessage);
						counter++;            
					}
				}
			}
			getContext().getCounter("Messages Statistics", "Messages sent during threshold surpassed " + thresholdSurpassed).increment(counter);
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#executePostMessageAction(org.apache.giraph.graph.Vertex)
		 */
		@Override
		protected
		void
		executePostMessageAction(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex) {
			if(messageCache != null){
				sendMessageToAll(messageCache, vertex, surpassedThreshold);    		  
				messageCache.clear();
			}
		}
	}

	public static class MultiScaleGraphExplorer extends DrawingBoundariesExplorer<AstralBodyCoordinateWritable, SpTreeEdgeValue>
	{

		/* (non-Javadoc)
		 * @see unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			super.compute(vertex, msgs);
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
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
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayerAggregator)).get();

		}
	}

	public static class MultiScaleGraphExplorerWithComponentsNo extends DrawingBoundariesExplorerWithComponentsNo<AstralBodyCoordinateWritable, SpTreeEdgeValue>
	{

		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			super.compute(vertex, msgs);
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
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
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayerAggregator)).get();
		}
	}

	public static class MultiScaleDrawingScaler extends DrawingScaler<AstralBodyCoordinateWritable, SpTreeEdgeValue>
	{


		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
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
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayerAggregator)).get();
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.LayoutRoutine.DrawingScaler#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			super.compute(vertex, msgs);
		}
	}

	public static class MultiScaleLayoutCC extends LayoutCCs<AstralBodyCoordinateWritable, SpTreeEdgeValue>
	{

		/* (non-Javadoc)
		 * @see unipg.gila.layout.LayoutRoutine.LayoutCCs#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			super.compute(vertex, msgs);
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
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
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayerAggregator)).get();;
		}
	}

}
