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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.FloatWritableArray;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
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
public class AbstractSeeder<I extends PartitionedLongWritable, V extends CoordinateWritable, E extends Writable, M1 extends LayoutMessage, M2 extends LayoutMessage> extends AbstractComputation<I, V, E, M1, M2> {

	float initialTemp;
	float accuracy;
	int ttlmax;
	
	MapWritable tempsMap;
	MapWritable sizesMap;
	
	boolean sendDegToo;
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph.graph.Vertex, java.lang.Iterable)
	 */
	@Override
	public void compute(Vertex<I, V, E> vertex, Iterable<M1> messages)
			throws IOException {
		CoordinateWritable vValue = vertex.getValue();

		if(getSuperstep() == 0){ //FIRST SUPERSTEP, EACH VERTEX BROADCASTS ITS COORDINATES TO ITS NEIGHBOR.
			aggregate(FloodingMaster.maxOneDegAggregatorString, new IntWritable(vValue.getOneDegreeVerticesQuantity()));
			
			gatherAndSend(vertex, vValue.getCoordinates());
			vValue.resetAnalyzed();
			return;
		}

		long component = vValue.getComponent();
		
		float coords[] = vValue.getCoordinates();	
		float[] forces = vValue.getForceVector();
		
		float displacementModule = Toolbox.computeModule(forces);
		float correctedDispModule;
		
		if(displacementModule > 0 && getSuperstep() > 2){
			
			float tempX;
			float tempY;
			
			float[] temps = ((FloatWritableArray)tempsMap.get(new LongWritable(component))).get();

			tempX = (forces[0] / displacementModule * Math.min(displacementModule, temps[0]));
			tempY = (forces[1] / displacementModule * Math.min(displacementModule, temps[1]));

			coords[0] += tempX;
			coords[1] += tempY;		

			vValue.setCoordinates(coords[0], coords[1]);

			correctedDispModule = Toolbox.computeModule(new float[]{tempX, tempY});

			
		}else
			correctedDispModule = 0;

		if((correctedDispModule < accuracy && getSuperstep() > 2) || getSuperstep() > FloodingMaster.maxSuperstep)
			aggregate(FloodingMaster.convergenceAggregatorString, new LongWritable(1));

		gatherAndSend(vertex, coords);
		vValue.resetAnalyzed();
	}

	protected void gatherAndSend(Vertex<I, V, E> vertex, float[] coords){
		LayoutMessage toSend = new LayoutMessage(vertex.getId().getId(), 
				ttlmax - 1,
				coords);
		if(sendDegToo)
			toSend.setDeg(vertex.getNumEdges()+vertex.getValue().getOneDegreeVerticesQuantity());
		sendMessageToAllEdges(vertex, (M2) toSend);	
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
	 */
	@Override
	public void initialize(GraphState graphState,
			WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor,
			GraphTaskManager<I, V, E> graphTaskManager,
			WorkerGlobalCommUsage workerGlobalCommUsage,
			WorkerContext workerContext) {
		super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
				workerGlobalCommUsage, workerContext);
		accuracy = getConf().getFloat(FloodingMaster.accuracyString, FloodingMaster.accuracyDefault);
		ttlmax = getConf().getInt(FloodingMaster.ttlMaxString, FloodingMaster.ttlMaxDefault);		

		tempsMap = getAggregatedValue(FloodingMaster.tempAGG);
		sizesMap = getAggregatedValue(FloodingMaster.correctedSizeAGG);
		
		sendDegToo = getConf().getBoolean(FloodingMaster.sendDegTooOptionString, false);
	}

}
