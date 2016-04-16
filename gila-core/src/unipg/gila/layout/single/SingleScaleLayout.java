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
package unipg.gila.layout.single;

import java.io.IOException;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.datastructures.messagetypes.SingleLayerLayoutMessage;
import unipg.gila.common.datastructures.messagetypes.LayoutMessageMatrix;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.layout.AbstractPropagator;
import unipg.gila.layout.AbstractSeeder;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer;
import unipg.gila.layout.LayoutRoutine.DrawingScaler;
import unipg.gila.layout.LayoutRoutine.LayoutCCs;

public class SingleScaleLayout {
	
	public static class Seeder extends AbstractSeeder<CoordinateWritable, IntWritable>{
		
		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractSeeder#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
		 */
		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, CoordinateWritable, IntWritable> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, CoordinateWritable, IntWritable> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
		}
		
		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractSeeder#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, CoordinateWritable, IntWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			super.compute(vertex, messages);
		}

	}
	

	/**
	 * The propagator class works as follows. For each message received:
	 * 
	 * 1) If the message is received from a vertex not analyzed yet then the acting force on it is computed.
	 * 	  If the vertex who sent the message is a neighbor then the attractive forces are computed; otherwise
	 * 	  only the repulsive ones.
	 * 
	 * 2) If the message TTL is > 0 it is decreased and the message is broadcasted to the vertex neighbors.
	 * 
	 * 3) If the messages queues are activated, a portion of the messages is popped from the queue and broadcasted.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class Propagator extends AbstractPropagator<CoordinateWritable, IntWritable>{


		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
		 */
		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, CoordinateWritable, IntWritable> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, CoordinateWritable, IntWritable> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, CoordinateWritable, IntWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			super.compute(vertex, messages);
		}
	}
	
}
