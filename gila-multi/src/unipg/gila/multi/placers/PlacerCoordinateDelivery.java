/**
 * 
 */
package unipg.gila.multi.placers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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
import unipg.gila.common.datastructures.messagetypes.SingleLayerLayoutMessage;
import unipg.gila.common.datastructures.messagetypes.LayoutMessageMatrix;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.MultiScaleComputation;

/**
 * @author Alessio Arleo
 *
 */
public class PlacerCoordinateDelivery extends MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage> {

	//LOGGER
	Logger log = Logger.getLogger(PlacerCoordinateDelivery.class);

	protected float k;

	/* (non-Javadoc)
	 * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	protected void vertexInLayerComputation(
			Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex,
			Iterable<LayoutMessage> msgs) throws IOException {
		boolean found = false;
		Iterator<LayoutMessage> ms = msgs.iterator();
		if(!ms.hasNext() || vertex.getValue().isSun())
			return;
		//		Iterator<Edge<LayeredPartitionedLongWritable, IntWritable>> edges = vertex.getEdges().iterator();
		//		while(edges.hasNext()){
		//			Edge<LayeredPartitionedLongWritable, IntWritable> currentEdge = edges.next();
		////			myNeighbors.put(currentEdge.getTargetVertexId().getId(), currentEdge.getTargetVertexId());
		//		}
		while(ms.hasNext() && !found){
			LayoutMessage current = (LayoutMessage) ms.next();
			if(current.getPayloadVertex().equals(vertex.getId())){
				vertex.getValue().setCoordinates(current.getValue()[0], current.getValue()[1]);
				found = true;
				if(SolarPlacerRoutine.logPlacer)
					log.info("Received my new coordinates! " + current.toString());
			}
		}
		float[] myCoords = vertex.getValue().getCoordinates();
		ms = msgs.iterator();
		while(ms.hasNext()){
			LayoutMessage current = (LayoutMessage) ms.next();
			if(!current.isAZombie() && !current.getPayloadVertex().equals(vertex.getId())){
				if(vertex.getEdgeValue(current.getPayloadVertex()) != null){
					if(SolarPlacerRoutine.logPlacer)						
						log.info("I'm propagating to my neighbor " + current.getPayloadVertex() + " the message" + current);
					if(current.getWeight() >= 0){
						if(SolarPlacerRoutine.logPlacer)
							log.info("Retransmitting to " + current.getPayloadVertex());
						sendMessage(current.getPayloadVertex().copy(), (LayoutMessage)current.propagateAndDie());
					}else{
						float angle = new Float(Math.random()*Math.PI*2);
						float desiredDistance = (float) ((IntWritable)vertex.getEdgeValue(current.getPayloadVertex())).get()*k;
						float[] blanks = new float[]{myCoords[0] + new Float(Math.cos(angle)*desiredDistance),
								myCoords[1] + new Float(Math.sin(angle)*desiredDistance)};
						if(SolarPlacerRoutine.logPlacer)
							log.info("Blanks received; generating random coordinates with coordinates" + blanks[0] + " " + blanks[1] + " starting from " + myCoords[0] + " " + myCoords[1]);
						sendMessage(current.getPayloadVertex().copy(), new LayoutMessage(current.getPayloadVertex().copy(), blanks));
					}
				}
			}
		}
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
		k = ((FloatWritable)getAggregatedValue(LayoutRoutine.k_agg)).get();

	}
}
