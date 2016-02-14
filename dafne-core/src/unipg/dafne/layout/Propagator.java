package unipg.dafne.layout;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import unipg.dafne.common.coordinatewritables.CoordinateWritable;
import unipg.dafne.common.datastructures.PartitionedLongWritable;
import unipg.dafne.common.datastructures.messagetypes.PlainMessage;
import unipg.dafne.utils.Toolbox;

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
public class Propagator extends AbstractComputation<PartitionedLongWritable, 
CoordinateWritable, NullWritable, PlainMessage, PlainMessage>{

	protected boolean useQueues;
	protected float minimumForceThreshold;


	protected Float k;
	protected float walshawConstant;
	private float queueFlushRatio;

	@Override
	public void initialize(
			GraphState graphState,
			WorkerClientRequestProcessor<PartitionedLongWritable, CoordinateWritable, NullWritable> workerClientRequestProcessor,
			GraphTaskManager<PartitionedLongWritable, CoordinateWritable, NullWritable> graphTaskManager,
			WorkerGlobalCommUsage workerGlobalCommUsage,
			WorkerContext workerContext) {
		super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
				workerGlobalCommUsage, workerContext);

		k = ((FloatWritable)getAggregatedValue(FloodingMaster.k_agg)).get();
		walshawConstant = ((FloatWritable)getAggregatedValue(FloodingMaster.walshawConstant_agg)).get();

		useQueues = getConf().getBoolean(FloodingMaster.useQueuesString, false);
		queueFlushRatio = getConf().getFloat(FloodingMaster.queuePercentageString, 0.1f);

	}

	@Override
	public void compute(
			Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
			Iterable<PlainMessage> messages)
					throws IOException {

		Iterator<PlainMessage> it = messages.iterator();
		CoordinateWritable vValue = vertex.getValue();

		float[] mycoords = vValue.getCoordinates();;	
		float[] foreigncoords;

		float distanceFromVertex;

		float[] force = new float[]{0.0f, 0.0f};
		float[] repulsiveForce = new float[]{0.0f, 0.0f};

		while(it.hasNext()){	
			PlainMessage currentMessage = it.next();

			LongWritable currentPayload = new LongWritable(currentMessage.getPayloadVertex());

			if(currentMessage.getPayloadVertex().equals(vertex.getId().getId()) || vValue.isAnalyzed(currentPayload))
				continue;

			foreigncoords=currentMessage.getValue();

			distanceFromVertex = Toolbox.computeModule(mycoords, foreigncoords);

			float deltaX = (foreigncoords[0] - mycoords[0]);
			float deltaY = (foreigncoords[1] - mycoords[1]);		

			float squareDistance = Toolbox.squareModule(mycoords, foreigncoords);		

			//ATTRACTIVE FORCES
			if(vValue.hasBeenReset()){				
				force[0] += deltaX*distanceFromVertex/k;
				force[1] += deltaY*distanceFromVertex/k;	
			}

			//REPULSIVE FORCES
			repulsiveForce[0] += (deltaX/squareDistance);
			repulsiveForce[1] += (deltaY/squareDistance);

			vValue.analyse(currentPayload);

			if(!currentMessage.isAZombie()){
				aggregate(FloodingMaster.MessagesAggregatorString, new BooleanWritable(false));
				if(!useQueues)
					sendMessageToAllEdges(vertex, (PlainMessage) currentMessage.propagate());					
				else
					vertex.getValue().enqueueMessage(currentMessage.propagate());	
			}

		}

		//REPULSIVE FORCE MODERATION
		repulsiveForce[0] *= walshawConstant;
		repulsiveForce[1] *= walshawConstant;

		force[0] -= repulsiveForce[0];
		force[1] -= repulsiveForce[1];

		vValue.setAsMoving();
		vValue.addToForceVector(force);

		if(!useQueues)
			return;

		Writable[] toDequeue = vValue.dequeueMessages(new Double(Math.ceil(queueFlushRatio*vertex.getNumEdges())).intValue());

		for(int i=0; i<toDequeue.length; i++){
			PlainMessage current = (PlainMessage) toDequeue[i];
			if(current != null){
				aggregate(FloodingMaster.MessagesAggregatorString, new BooleanWritable(false));
				sendMessageToAllEdges(vertex, current);
			}
			else
				break;
		}

	}

}
