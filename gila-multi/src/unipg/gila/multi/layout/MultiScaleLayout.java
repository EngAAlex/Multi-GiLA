/**
 * 
 */
package unipg.gila.multi.layout;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.datastructures.messagetypes.LayoutMessageMatrix;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.layout.AbstractPropagator;
import unipg.gila.layout.AbstractSeeder;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorerWithComponentsNo;
import unipg.gila.layout.LayoutRoutine.DrawingScaler;
import unipg.gila.layout.LayoutRoutine.LayoutCCs;
import unipg.gila.multi.coarseners.SolarMergerRoutine;

/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleLayout {

	protected static Logger log = Logger.getLogger(MultiScaleLayout.class);

	protected static int currentLayer;	
	
	public static class Seeder extends AbstractSeeder<AstralBodyCoordinateWritable, IntWritable>{

		private float k;
		
		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractSeeder#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
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
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get();
			k = ((FloatWritable)getAggregatedValue(LayoutRoutine.k_agg)).get();
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractSeeder#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			else{
				log.info(vertex.getId() + " computing hiuar");
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
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				LayoutMessage message) {
			Iterator<Edge<LayeredPartitionedLongWritable, IntWritable>> edges = vertex.getEdges().iterator();
			while(edges.hasNext()){
				LayeredPartitionedLongWritable current = edges.next().getTargetVertexId();
				if(current.getLayer() == currentLayer){
					aggregate(LayoutRoutine.max_K_agg, new FloatWritable(((IntWritable)vertex.getEdgeValue(current)).get()*k));
					LayoutMessage msgCopy = ((LayoutMessage)message).copy();
					log.info("Sending " + msgCopy.toString() + " to " + current);
					sendMessage(current, msgCopy);
				}
			}
		}


	}

	public static class Propagator extends AbstractPropagator<AstralBodyCoordinateWritable, IntWritable>{

		float modifier;
		float maxK = Float.MIN_VALUE;
		
		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
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
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get();
			modifier = getConf().getFloat(LayoutRoutine.walshawModifierString, LayoutRoutine.walshawModifierDefault);
			maxK = ((FloatWritable)getAggregatedValue(LayoutRoutine.max_K_agg)).get();
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			else{
				if(LayoutRoutine.logLayout)
					log.info(vertex.getId() + " computing hiuar");
				if(new Float(vertex.getValue().getCoordinates()[0]).isNaN() || new Float(vertex.getValue().getCoordinates()[1]).isNaN())
					throw new IOException("NAN detected");
				super.compute(vertex, messages);
			}
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#requestOptimalEdgeLength(org.apache.giraph.graph.Vertex, unipg.gila.common.datastructures.PartitionedLongWritable)
		 */
		@Override
		protected float requestOptimalSpringLength(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				LayeredPartitionedLongWritable currentPayload) {
			if(LayoutRoutine.logLayout)
				log.info("Suggesting a spring length of " + ((IntWritable)vertex.getEdgeValue(currentPayload)).get()*k + " based on " + ((IntWritable)vertex.getEdgeValue(currentPayload)).get() + " and " + k);
//			if(currentLayer > 0)
//				return ((IntWritable)vertex.getEdgeValue(currentPayload)).get()*k;
//			else
				return ((IntWritable)vertex.getEdgeValue(currentPayload)).get()*k;
		}
		
		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#requestWalshawConstant()
		 */
		@Override
		protected float requestWalshawConstant() {
			if(LayoutRoutine.logLayout)
				log.info("Suggested walshawConstant " + Math.pow(maxK,2)*modifier + " from " + Math.pow(maxK,2) + " " + modifier);;
			return (float) (Math.pow(maxK,2)*modifier);
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#sendMessageToAllEdges(org.apache.giraph.graph.Vertex, org.apache.hadoop.io.Writable)
		 */
		@Override
		public void sendMessageToAllEdges(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				LayoutMessage message) {
			Iterator<Edge<LayeredPartitionedLongWritable, IntWritable>> edges = vertex.getEdges().iterator();
			while(edges.hasNext()){
				LayeredPartitionedLongWritable current = edges.next().getTargetVertexId();
				LayoutMessage msgCopy = ((LayoutMessage)message).copy();
				sendMessage(current, msgCopy);;
			}
		}
	}


	public static class MultiScaleGraphExplorer extends DrawingBoundariesExplorer<AstralBodyCoordinateWritable, IntWritable>
	{

		/* (non-Javadoc)
		 * @see unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
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
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get();

		}
	}

	public static class MultiScaleGraphExplorerWithComponentsNo extends DrawingBoundariesExplorerWithComponentsNo<AstralBodyCoordinateWritable, IntWritable>
	{

		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
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
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get();
		}
	}

	public static class MultiScaleDrawingScaler extends DrawingScaler<AstralBodyCoordinateWritable, IntWritable>
	{


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
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get();
		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.LayoutRoutine.DrawingScaler#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			super.compute(vertex, msgs);
		}
	}

	public static class MultiScaleLayoutCC extends LayoutCCs<AstralBodyCoordinateWritable, IntWritable>
	{
		
		/* (non-Javadoc)
		 * @see unipg.gila.layout.LayoutRoutine.LayoutCCs#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
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
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
			currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get();;
		}
	}

}
