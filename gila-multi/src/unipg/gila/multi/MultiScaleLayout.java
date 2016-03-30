/**
 * 
 */
package unipg.gila.multi;

import java.io.IOException;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.layout.AbstractPropagator;
import unipg.gila.layout.AbstractSeeder;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer;
import unipg.gila.layout.LayoutRoutine.DrawingScaler;
import unipg.gila.layout.LayoutRoutine.LayoutCCs;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.common.AstralBodyCoordinateWritable;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;

/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleLayout {
	
	public static class Seeder extends AbstractSeeder<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable, LayoutMessage, LayoutMessage>{
		
		protected int currentLayer;
		
		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractSeeder#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
		 */
		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> graphTaskManager,
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
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			else{
				super.compute(vertex, messages);
			}
		}
		
	}

	public static class Propagator extends AbstractPropagator<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable, LayoutMessage, LayoutMessage>{
	
		protected int currentLayer;

		/* (non-Javadoc)
		 * @see unipg.gila.layout.AbstractPropagator#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
		 */
		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> graphTaskManager,
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
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			if(vertex.getId().getLayer() != currentLayer)
				return;
			else{
				super.compute(vertex, messages);
			}
		}
	}
	
	public static class MultiScaleGraphExplorer extends DrawingBoundariesExplorer<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable, LayoutMessage, LayoutMessage>
	{}
	
	public static class MultiScaleDrawingScaler extends DrawingScaler<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable, LayoutMessage, LayoutMessage>
	{}
	
	public static class MultiScaleLayoutCCs extends LayoutCCs<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable, LayoutMessage, LayoutMessage>
	{}

}
