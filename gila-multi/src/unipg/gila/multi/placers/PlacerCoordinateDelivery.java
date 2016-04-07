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

import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.common.AstralBodyCoordinateWritable;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;

/**
 * @author Alessio Arleo
 *
 */
public class PlacerCoordinateDelivery extends MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage> {

	/* (non-Javadoc)
	 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	protected void vertexInLayerComputation(
			Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
			Iterable<LayoutMessage> msgs) throws IOException {
		Iterator<LayoutMessage> ms = msgs.iterator();
		if(!ms.hasNext() || vertex.getValue().isSun())
			return;
		Long currentVertexID = vertex.getId().getId();
		HashMap<Long, LayeredPartitionedLongWritable> myNeighbors = new HashMap<Long, LayeredPartitionedLongWritable>();
		Iterator<Edge<LayeredPartitionedLongWritable, FloatWritable>> edges = vertex.getEdges().iterator();
		while(edges.hasNext()){
			Edge<LayeredPartitionedLongWritable, FloatWritable> currentEdge = edges.next();
			myNeighbors.put(currentEdge.getTargetVertexId().getId(), currentEdge.getTargetVertexId());
		}
		while(ms.hasNext()){
			LayoutMessage current = ms.next();
			if(current.getPayloadVertex().equals(currentVertexID))
				vertex.getValue().setCoordinates(vertex.getValue().getCoordinates()[0] + current.getValue()[0], vertex.getValue().getCoordinates()[1] + current.getValue()[1]);
			else
				if(!current.isAZombie())
					if(myNeighbors.containsKey(current.getPayloadVertex()))
						sendMessage(myNeighbors.get(current.getPayloadVertex()), (LayoutMessage)current.propagateAndDie());				
		}
	}

}
