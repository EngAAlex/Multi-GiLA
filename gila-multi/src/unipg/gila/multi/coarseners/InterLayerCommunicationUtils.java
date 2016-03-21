/**
 * 
 */
package unipg.gila.multi.coarseners;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;

import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.common.AstralBodyCoordinateWritable;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;
import unipg.gila.multi.common.SolarMessage;

public class InterLayerCommunicationUtils{

	/**
	 * This computation broadcasts the vertex coordinates before transferring them to the underlying layer at the next superstep.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class coordinatesBroadcast extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, LayoutMessage>{

		/* (non-Javadoc)
		 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
				Iterable<SolarMessage> msgs) throws IOException {
			if(vertex.getValue().getLowerLevelWeight() > 0)
				sendMessageToAllEdges(vertex, new LayoutMessage(vertex.getId().getId(), vertex.getValue().getCoordinates()));
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

		/* (non-Javadoc)
		 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			AstralBodyCoordinateWritable value = vertex.getValue();
			if(value.getLowerLevelWeight() > 0){
				LayeredPartitionedLongWritable mineId = vertex.getId();
				LayeredPartitionedLongWritable lowerID = new LayeredPartitionedLongWritable(mineId.getPartition(), mineId.getId(), mineId.getLayer() - 1);
				sendMessage(lowerID, new LayoutMessage(mineId.getId(), value.getCoordinates()));
				Iterator<LayoutMessage> it = msgs.iterator();
				while(it.hasNext())
					sendMessage(lowerID, (LayoutMessage) it.next().propagateAndDie());
			}
		}
	}
}
