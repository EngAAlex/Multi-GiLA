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

import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.layout.AbstractPropagator;
import unipg.gila.layout.AbstractSeeder;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer.DrawingBoundariesExplorerWithComponentsNo;
import unipg.gila.layout.LayoutRoutine.DrawingScaler;
import unipg.gila.layout.LayoutRoutine.LayoutCCs;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.common.AstralBodyCoordinateWritable;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;
import unipg.gila.multi.common.SuperLayoutMessage;

/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleLayout {
	
	protected static Logger log = Logger.getLogger(MultiScaleLayout.class);
	
	public static class Seeder extends AbstractSeeder<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, SuperLayoutMessage, SuperLayoutMessage>{
		
		protected int currentLayer;
		
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
		}
		
		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractSeeder#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<SuperLayoutMessage> messages) throws IOException {
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
				SuperLayoutMessage message) {
			Iterator<Edge<LayeredPartitionedLongWritable, IntWritable>> edges = vertex.getEdges().iterator();
			while(edges.hasNext()){
				LayeredPartitionedLongWritable current = edges.next().getTargetVertexId();
				if(current.getLayer() == currentLayer)
					sendMessage(current, message);
			}
		}
		
	}

	public static class Propagator extends AbstractPropagator<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, SuperLayoutMessage, SuperLayoutMessage>{
	
		protected int currentLayer;

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

		}

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<SuperLayoutMessage> messages) throws IOException {
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
		 * @see unipg.gila.layout.AbstractPropagator#requestOptimalEdgeLength(org.apache.giraph.graph.Vertex, unipg.gila.common.datastructures.PartitionedLongWritable)
		 */
		@Override
		protected float requestOptimalEdgeLength(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				LayeredPartitionedLongWritable currentPayload) {
			if(currentLayer > 0)
				return super.requestOptimalEdgeLength(vertex, currentPayload);
			else
				return super.requestOptimalEdgeLength(vertex, currentPayload)*k;
		}
		
		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#sendMessageToAllEdges(org.apache.giraph.graph.Vertex, org.apache.hadoop.io.Writable)
		 */
		@Override
		public void sendMessageToAllEdges(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				SuperLayoutMessage message) {
			Iterator<Edge<LayeredPartitionedLongWritable, IntWritable>> edges = vertex.getEdges().iterator();
			while(edges.hasNext()){
				LayeredPartitionedLongWritable current = edges.next().getTargetVertexId();
				if(current.getLayer() == currentLayer)
					sendMessage(current, message);
			}
		}
	}
	
	
	public static class MultiScaleGraphExplorer extends DrawingBoundariesExplorer<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, SuperLayoutMessage, SuperLayoutMessage>
	{
		protected int currentLayer;
		
		/* (non-Javadoc)
		 * @see unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<SuperLayoutMessage> msgs) throws IOException {
			if(vertex.getId().getLayer() == currentLayer)
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
		
		public class MultiScaleGraphExplorerWithComponentsNo extends DrawingBoundariesExplorerWithComponentsNo<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, SuperLayoutMessage, SuperLayoutMessage>
		{
			/* (non-Javadoc)
			 * @see unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer.DrawingBoundariesExplorerWithComponentsNo#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
			 */
			@Override
			public void compute(
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
					Iterable<SuperLayoutMessage> msgs) throws IOException {
				if(vertex.getId().getLayer() == currentLayer)
					return;
				super.compute(vertex, msgs);
			}
		}
		
		public class MultiScaleDrawingScaler extends DrawingScaler<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, SuperLayoutMessage, SuperLayoutMessage>
		{
			/* (non-Javadoc)
			 * @see unipg.gila.layout.LayoutRoutine.DrawingScaler#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
			 */
			@Override
			public void compute(
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
					Iterable<SuperLayoutMessage> msgs) throws IOException {
				if(vertex.getId().getLayer() == currentLayer)
					return;
				super.compute(vertex, msgs);
			}
		}
		
		public class MultiScaleLayoutCC extends LayoutCCs<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable, SuperLayoutMessage, SuperLayoutMessage>
		{
			/* (non-Javadoc)
			 * @see unipg.gila.layout.LayoutRoutine.LayoutCCs#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
			 */
			@Override
			public void compute(
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
					Iterable<SuperLayoutMessage> msgs) throws IOException {
				if(vertex.getId().getLayer() == currentLayer)
					return;
				super.compute(vertex, msgs);
			}
		}
		
	}

//	
//	public static class MultiScaleLayoutCCs extends LayoutCCs<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable, LayoutMessage, LayoutMessage>
//	{}

}
