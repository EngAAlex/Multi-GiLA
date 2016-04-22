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
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.datastructures.messagetypes.SingleLayerLayoutMessage;
import unipg.gila.common.datastructures.messagetypes.LayoutMessageMatrix;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.multi.MultiScaleComputation;

/**
 * @author Alessio Arleo
 *
 */
public class PlacerCoordinateDelivery extends MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage> {

	//LOGGER
	Logger log = Logger.getLogger(PlacerCoordinateDelivery.class);
	
	/* (non-Javadoc)
	 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	protected void vertexInLayerComputation(
			Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
			Iterable<LayoutMessage> msgs) throws IOException {
		Iterator<LayoutMessage> ms = msgs.iterator();
		if(!ms.hasNext() || vertex.getValue().isSun())
			return;
//		Iterator<Edge<LayeredPartitionedLongWritable, IntWritable>> edges = vertex.getEdges().iterator();
//		while(edges.hasNext()){
//			Edge<LayeredPartitionedLongWritable, IntWritable> currentEdge = edges.next();
////			myNeighbors.put(currentEdge.getTargetVertexId().getId(), currentEdge.getTargetVertexId());
//		}
		while(ms.hasNext()){
			LayoutMessage current = (LayoutMessage) ms.next();
			if(current.getPayloadVertex().equals(vertex.getId())){
				vertex.getValue().setCoordinates(current.getValue()[0], current.getValue()[1]);
				if(SolarPlacerRoutine.logPlacer)
					log.info("Received my new coordinates! " + current.toString());
			}else
				if(!current.isAZombie())
					if(vertex.getEdgeValue(current.getPayloadVertex()) != null){
						if(SolarPlacerRoutine.logPlacer)						
							log.info("I'm propagating to my neighbor " + current.getPayloadVertex() + " the message" + current);
						sendMessage(current.getPayloadVertex(), (LayoutMessage)current.propagateAndDie());
					}
		}
	}

}
