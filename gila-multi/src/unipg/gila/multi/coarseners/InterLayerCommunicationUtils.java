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

import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.common.AstralBodyCoordinateWritable;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;
import unipg.gila.multi.common.SolarMessage;
import unipg.gila.multi.common.SuperLayoutMessage;
import unipg.gila.partitioning.Spinner;

public class InterLayerCommunicationUtils{

	/**
	 * This computation broadcasts the vertex coordinates before transferring them to the underlying layer at the next superstep.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class CoordinatesBroadcast extends MultiScaleComputation<AstralBodyCoordinateWritable, SuperLayoutMessage, SuperLayoutMessage>{

		/* (non-Javadoc)
		 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<SuperLayoutMessage> msgs) throws IOException {
			if(vertex.getValue().getLowerLevelWeight() > 0)
				sendMessageToAllEdges(vertex, new SuperLayoutMessage(vertex.getId(), vertex.getValue().getCoordinates()));
			}
	}
	
	/**
	 * This computation transfers the data about the upper level vertices to the lower level ones.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class InterLayerDataTransferComputation extends
	MultiScaleComputation<AstralBodyCoordinateWritable, SuperLayoutMessage, SuperLayoutMessage> {

		/* (non-Javadoc)
		 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<SuperLayoutMessage> msgs) throws IOException {
			AstralBodyCoordinateWritable value = vertex.getValue();
			if(value.getLowerLevelWeight() > 0){
				LayeredPartitionedLongWritable mineId = vertex.getId();
				LayeredPartitionedLongWritable lowerID = new LayeredPartitionedLongWritable(mineId.getPartition(), mineId.getId(), mineId.getLayer() - 1);
				sendMessage(lowerID, new SuperLayoutMessage(mineId, value.getCoordinates()));
				Iterator<SuperLayoutMessage> it = msgs.iterator();
				while(it.hasNext())
					sendMessage(lowerID, (SuperLayoutMessage) it.next().propagateAndDie());
			}
			//REACTIVATE TO PURGE UPPER LAYERS
//			vertex.getValue().clearAstralInfo();
//			removeVertexRequest(vertex.getId());
		}
	}
	

	public static class MergerToPlacerDummyComputation extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SuperLayoutMessage>{

		Random rnd;
		
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
				Iterable<SolarMessage> msgs) throws IOException {
			float bBoxX = getConf().getFloat(Spinner.bBoxStringX, 1200.0f);
			float bBoxY = getConf().getFloat(Spinner.bBoxStringY, bBoxX);
			vertex.getValue().setCoordinates(rnd.nextFloat()*bBoxX, rnd.nextFloat()*bBoxY);
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
		}
	}
}
