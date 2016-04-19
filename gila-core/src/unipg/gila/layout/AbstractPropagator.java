/**
 * 
 */
package unipg.gila.layout;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.datastructures.messagetypes.SingleLayerLayoutMessage;
import unipg.gila.common.datastructures.messagetypes.LayoutMessageMatrix;
import unipg.gila.common.datastructures.messagetypes.MessageWritable;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.layout.force.FR;
import unipg.gila.layout.force.Force;
import unipg.gila.utils.Toolbox;


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
public class AbstractPropagator<V extends CoordinateWritable, E extends IntWritable> extends AbstractComputation<LayeredPartitionedLongWritable, V, E, LayoutMessage, LayoutMessage> {

	//LOGGER
	Logger log = Logger.getLogger(this.getClass());
	
	protected boolean useQueues;
	protected float minimumForceThreshold;
	protected float walshawConstant;
	private float queueFlushRatio;
	
	protected float k;

	protected Force force;

	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void compute(Vertex<LayeredPartitionedLongWritable, V, E> vertex, Iterable<LayoutMessage> messages)
			throws IOException {
		Iterator<LayoutMessage> it = messages.iterator();
		CoordinateWritable vValue = vertex.getValue();

		float[] mycoords = vValue.getCoordinates();;	
		float[] foreigncoords;

		float distance;

		float[] finalForce = new float[]{0.0f, 0.0f};
		float[] repulsiveForce = new float[]{0.0f, 0.0f};
		float[] tempForce = new float[]{0.0f, 0.0f};;

		int v1Deg;
		int v2Deg;		

		v1Deg = vertex.getNumEdges() + vValue.getWeight();

		while(it.hasNext()){	
			LayoutMessage currentMessage = it.next();

			LayeredPartitionedLongWritable currentPayload = currentMessage.getPayloadVertex();
			log.info("Message payload" + currentPayload);

			if(currentPayload.equals(vertex.getId()) || vValue.isAnalyzed(currentPayload))
				continue;

			foreigncoords = currentMessage.getValue();
			
			log.info("Received coordinates " + foreigncoords[0] + " " + foreigncoords[1] + " from " + currentMessage.getPayloadVertex());

			float squareDistance = Toolbox.squareModule(mycoords, foreigncoords);
			distance = (float) Math.sqrt(squareDistance);

			float deltaX = (foreigncoords[0] - mycoords[0]);
			float deltaY = (foreigncoords[1] - mycoords[1]);		

			v2Deg = currentMessage.getWeight();
			
			
//			float weightRatio = v2Deg/(float)v1Deg;
			
//			float weightRatio = 1;
			
//			log.info("Ma weight vs theirs " + v1Deg + " " + v2Deg + " " + weightRatio);
			
			//ATTRACTIVE FORCES
			if(vValue.hasBeenReset()){
				tempForce = force.computeAttractiveForce(deltaX, deltaY, distance, squareDistance, requestOptimalSpringLength(vertex, currentPayload), v1Deg, v2Deg);				
				finalForce[0] += tempForce[0];
				finalForce[1] += tempForce[1];
				log.info("computed attractive " + finalForce[0] + " " + finalForce[1] + " with data " + deltaX + " " + deltaY + " " + distance + " " + requestOptimalSpringLength(vertex, currentPayload));
//				finalForce[0] += (computedForce*cos);
//				finalForce[1] += (computedForce*sin);
			}

			//REPULSIVE FORCES
			tempForce = force.computeRepulsiveForce(deltaX, deltaY, distance, squareDistance, v1Deg, v2Deg);

			log.info("computed repulsive " + tempForce[0] + " " + tempForce[1] + " with data " + deltaX + " " + deltaY + " " + distance);

			repulsiveForce[0] += tempForce[0];
			repulsiveForce[1] += tempForce[1];

			vValue.analyze(currentPayload.getId());

			if(!currentMessage.isAZombie()){
				aggregate(LayoutRoutine.MessagesAggregatorString, new BooleanWritable(false));
				if(!useQueues)
					sendMessageToAllEdges(vertex, (LayoutMessage) currentMessage.propagate());					
				else
					vertex.getValue().enqueueMessage(currentMessage.propagate());	
			}

		}

		log.info("Going to moderate on " + walshawConstant + " from " + repulsiveForce[0] + " " + repulsiveForce[1]);
		
		//REPULSIVE FORCE MODERATION
		repulsiveForce[0] *= requestWalshawConstant();
		repulsiveForce[1] *= requestWalshawConstant();

		finalForce[0] -= repulsiveForce[0];
		finalForce[1] -= repulsiveForce[1];

		log.info("computed repuslive " + repulsiveForce[0] + " " + repulsiveForce[1] + " final " + finalForce[0] + " " + finalForce[1]);
		
		vValue.setAsMoving();
		vValue.addToForceVector(finalForce);

		if(!useQueues)
			return;

		Writable[] toDequeue = vValue.dequeueMessages(new Double(Math.ceil(queueFlushRatio*vertex.getNumEdges())).intValue());

		for(int i=0; i<toDequeue.length; i++){
			LayoutMessage current = (LayoutMessage) toDequeue[i];
			if(current != null){
				aggregate(LayoutRoutine.MessagesAggregatorString, new BooleanWritable(false));
				sendMessageToAllEdges(vertex, (LayoutMessage) current);
			}
			else
				break;
		}

	}

	/**
	 * @return
	 */
	protected float requestOptimalSpringLength(Vertex<LayeredPartitionedLongWritable,V,E> vertex, LayeredPartitionedLongWritable currentPayload) {
		return k;
	}
	
	protected float requestWalshawConstant(){
		return walshawConstant;
	}

	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void initialize(GraphState graphState,
			WorkerClientRequestProcessor<LayeredPartitionedLongWritable, V, E> workerClientRequestProcessor,
			GraphTaskManager<LayeredPartitionedLongWritable, V, E> graphTaskManager,
			WorkerGlobalCommUsage workerGlobalCommUsage,
			WorkerContext workerContext) {
		super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
				workerGlobalCommUsage, workerContext);
		k = ((FloatWritable)getAggregatedValue(LayoutRoutine.k_agg)).get();
		walshawConstant = ((FloatWritable)getAggregatedValue(LayoutRoutine.walshawConstant_agg)).get();

		useQueues = getConf().getBoolean(LayoutRoutine.useQueuesString, false);
		queueFlushRatio = getConf().getFloat(LayoutRoutine.queueUnloadFactor, 0.1f);

		try {
			force = ((Class<Force>)Class.forName(getConf().get(LayoutRoutine.forceMethodOptionString, FR.class.toString()))).newInstance();
		} catch (Exception e) {
			force = new FR();
		}
		force.generateForce(getConf().getStrings(LayoutRoutine.forceMethodOptionExtraOptionsString, ""));
	}

}
