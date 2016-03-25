/**
 * 
 */
package unipg.gila.multi.placers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.common.AstralBodyCoordinateWritable;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;

/**
 * @author Alessio Arleo
 *
 */
public class BodiesCoordinatesDelivery extends MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage> {

	/* (non-Javadoc)
	 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	protected void vertexInLayerComputation(
			Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
			Iterable<LayoutMessage> msgs) throws IOException {
		if(vertex.getValue().isAssigned())
			return;
		else
			vertex.getValue().setAssigned();
		AstralBodyCoordinateWritable value = vertex.getValue();
		Iterator<LayoutMessage> msgIt = msgs.iterator();
		HashMap<Long,LayeredPartitionedLongWritable> neighs = new HashMap<Long,LayeredPartitionedLongWritable>();
		Iterator<Edge<LayeredPartitionedLongWritable, FloatWritable>> edgesIt = vertex.getEdges().iterator();
		while(edgesIt.hasNext()){
			Edge<LayeredPartitionedLongWritable, FloatWritable> currentEdge = edgesIt.next();
			neighs.put(currentEdge.getTargetVertexId().getId(), currentEdge.getTargetVertexId());
		}
		while(msgIt.hasNext()){
			LayoutMessage current = msgIt.next();
			if(current.getPayloadVertex().equals(vertex.getId())){
				value.setCoordinates(current.getValue()[0], current.getValue()[1]);
			}else
				if(!current.isAZombie() && neighs.containsKey(current.getPayloadVertex())){
					sendMessage(neighs.get(current.getPayloadVertex()), (LayoutMessage) current.propagateAndDie());
				}
		}
	}

}
