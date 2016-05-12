/**
 * 
 */
package unipg.gila.layout;

import java.io.IOException;

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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.FloatWritableArray;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.datastructures.messagetypes.SingleLayerLayoutMessage;
import unipg.gila.common.datastructures.messagetypes.MessageWritable;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.utils.Toolbox;

/**
 * The seeding class works as follows.
 * 
 * = At the first superstep each vertex just broadcasts its coordinates to its neigbors.
 * = At every other superstep each vertex moderates the force vector acting on it and notifies if it moves less than the defined threshold set using "layout.accuracy" and 
 * then broadcasts its updated coordinates.
 * 
 * 
 * @author Alessio Arleo
 *
 */
public abstract class AbstractSeeder<V extends CoordinateWritable, E extends IntWritable>
	extends AbstractComputation<LayeredPartitionedLongWritable, V, E, LayoutMessage, LayoutMessage> {

	float initialTemp;
	float accuracy;
	int ttlmax;
	
	MapWritable tempsMap;
	MapWritable sizesMap;
	
	boolean sendDegToo;
	
	//LOGGER
	Logger log = Logger.getLogger(AbstractSeeder.class);
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<LayeredPartitionedLongWritable, V, E> vertex, Iterable<LayoutMessage> messages)
			throws IOException {
		CoordinateWritable vValue = vertex.getValue();

		if(getSuperstep() == 0){ //FIRST SUPERSTEP, EACH VERTEX BROADCASTS ITS COORDINATES TO ITS NEIGHBOR.
			aggregate(LayoutRoutine.maxOneDegAggregatorString, new IntWritable(vValue.getOneDegreeVerticesQuantity()));
			
			gatherAndSend(vertex, vValue.getCoordinates());
			vValue.resetAnalyzed();
			return;
		}

		int component = vValue.getComponent();
		
		float coords[] = vValue.getCoordinates();	
		float[] forces = vValue.getForceVector();
		
		float displacementModule = Toolbox.computeModule(forces);
		float correctedDispModule;
		
		if(displacementModule > 0){
			
			float tempX;
			float tempY;
			
			float[] temps = ((FloatWritableArray)tempsMap.get(new IntWritable(component))).get();

			tempX = (forces[0] / displacementModule * Math.min(displacementModule, temps[0]));
			tempY = (forces[1] / displacementModule * Math.min(displacementModule, temps[1]));

			coords[0] += tempX;
			coords[1] += tempY;		

			vValue.setCoordinates(coords[0], coords[1]);

			correctedDispModule = Toolbox.computeModule(new float[]{tempX, tempY});

			
		}else
			correctedDispModule = 0;
		if(LayoutRoutine.logLayout)
			log.info("Seeder here, displacement for vertex :" + vertex.getId() + " " + correctedDispModule );
		if(correctedDispModule < accuracy)// || LayoutRoutine.relativeSupersteps > LayoutRoutine.maxSuperstep)
			aggregate(LayoutRoutine.convergenceAggregatorString, new LongWritable(1));

		gatherAndSend(vertex, coords);
		vValue.resetAnalyzed();
	}

	protected void gatherAndSend(Vertex<LayeredPartitionedLongWritable, V, E> vertex, float[] coords){
		LayoutMessage toSend = new LayoutMessage();
		toSend.setPayloadVertex(vertex.getId());
		toSend.setTTL(ttlmax - 1);
		toSend.setValue(coords);
		toSend.setWeight(vertex.getValue().getWeight());
		sendMessageToAllEdges(vertex, toSend);	
		aggregate(LayoutRoutine.MessagesAggregatorString, new BooleanWritable(false));
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
	 */
	@Override
	public void initialize(GraphState graphState,
			WorkerClientRequestProcessor<LayeredPartitionedLongWritable, V, E> workerClientRequestProcessor,
			GraphTaskManager<LayeredPartitionedLongWritable, V, E> graphTaskManager,
			WorkerGlobalCommUsage workerGlobalCommUsage,
			WorkerContext workerContext) {
		super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
				workerGlobalCommUsage, workerContext);
//		accuracy = getConf().getFloat(LayoutRoutine.accuracyString, LayoutRoutine.accuracyDefault);
		accuracy = ((FloatWritable)getAggregatedValue(LayoutRoutine.currentAccuracyAggregator)).get();
		ttlmax = ((IntWritable)getAggregatedValue(LayoutRoutine.ttlMaxAggregator)).get();		

		tempsMap = getAggregatedValue(LayoutRoutine.tempAGG);
		sizesMap = getAggregatedValue(LayoutRoutine.correctedSizeAGG);
		
		sendDegToo = getConf().getBoolean(LayoutRoutine.sendDegTooOptionString, false);
	}

}
