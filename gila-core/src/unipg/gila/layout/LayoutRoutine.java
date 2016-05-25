/*******************************************************************************
 * Copyright 2016 Alessio Arleo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package unipg.gila.layout;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.FloatMaxAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.aggregators.ComponentAggregatorAbstract.ComponentFloatXYMaxAggregator;
import unipg.gila.aggregators.ComponentAggregatorAbstract.ComponentFloatXYMinAggregator;
import unipg.gila.aggregators.ComponentAggregatorAbstract.ComponentIntSumAggregator;
import unipg.gila.aggregators.ComponentAggregatorAbstract.ComponentMapOverwriteAggregator;
import unipg.gila.aggregators.LongWritableSetAggregator;
import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.FloatWritableArray;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.coolingstrategies.CoolingStrategy;
import unipg.gila.coolingstrategies.LinearCoolingStrategy;
import unipg.gila.utils.Toolbox;

/**
 * This class defines the behaviour of the layout phase of the algorithm, loading the appropriate computations at the right time. It defines
 * the stopping conditions, changes between the seeding and propagating phases and finally reintegrate the one-degree vertices before 
 * halting the computation.
 * 
 * @author general
 *
 */
@SuppressWarnings("rawtypes")
public class LayoutRoutine {

	//LOGGER
	Logger log = Logger.getLogger(this.getClass());

	//#############CLINT OPTIONS
	
	//LOGGING OPTIONS
	public static final String logLayoutString = "layout.logLayout";

	//COMPUTATION OPTIONS
	public static final String ttlMaxString = "layout.flooding.ttlMax";
	public static final String computationLimit = "layout.limit";
	public static final String convergenceThresholdString = "layout.convergence-threshold";
	public static final int ttlMaxDefault = 3;
	public static final int maxSstepsDefault = 1500;
	public static final float defaultConvergenceThreshold = 0.85f;

	//MESSAGES OPTIONS
	public static final String useQueuesString = "flooding.useQueues";
	public static final String queueUnloadFactor = "layout.queueUnloadFactor";
	public static final float queueUnloadFactorDefault = 0.1f;

	//REINTEGRATION OPTIONS
	public static final String radiusString = "reintegration.radius";
	public static final String dynamicRadiusString = "reintegration.dynamicRadius";
	public static final String coneWidth = "reintegration.coneWidth";
	public static final String paddingString = "reintegration.anglePadding";
	public static final String oneDegreeReintegratingClassOption = "reintegration.reintegratingClass";
	public static final String componentPaddingConfString = "reintegration.componentPadding";
	public static final String minimalAngularResolutionString = "reintegration.minimalAngularResolution";
	public static final String lowThresholdString = "reintegration.fairLowThreshold";
	public static final float lowThresholdDefault = 2.0f;
	public static final float defaultPadding = 20.0f;
	public static final float radiusDefault = 0.2f;	
	public static final float coneWidthDefault = 90.0f;	

	//DRAWING OPTIONS
	public final static String node_length = "layout.node_length";
	public final static String node_width = "layout.node_width";
	public final static String node_separation = "layout.node_separation";
	public final String initialTempFactorString = "layout.initialTempFactor";
	public static final String coolingSpeed = "layout.coolingSpeed";
	public static final String walshawModifierString = "layout.walshawModifier";
	public static final String accuracyString = "layout.accuracy";
	public static final float walshawModifierDefault = 0.052f;
	public final static float defaultNodeValue = 20.0f;
	public float defaultInitialTempFactor = 2.0f;
	public final String defaultCoolingSpeed = "0.93";
	public static final float accuracyDefault = 0.01f;
	public static final String forceMethodOptionString = "layout.forceModel";
	public static final String forceMethodOptionExtraOptionsString = "layout.forceModel.extraOptions";
	public static final String sendDegTooOptionString = "layout.sendDegreeIntoLayoutMessages";
	public static final String repulsiveForceModerationString = "layout.repulsiveForceModerationFactor";	

	//INPUT OPTIONS
	public static final String bbString = "layout.boundingBox";
	public static final String randomPlacementString = "layout.randomPlacement";

	//OUTPUT OPTIONS
	public static final String showPartitioningString = "layout.output.showPartitioning";
	public static final String showComponentString = "layout.output.showComponent";

	//AGGREGATORS
	public static final String convergenceAggregatorString = "AGG_TEMPERATURE";
	public static final String MessagesAggregatorString = "AGG_MESSAGES";
	public static final String maxOneDegAggregatorString = "AGG_ONEDEG_MAX";
	public final static String k_agg = "K_AGG";
	public static final String max_K_agg = "MAX_K_AGG";
	public static final String walshawConstant_agg = "WALSHAW_AGG";
	public final static String maxCoords = "AGG_MAX_COORDINATES";
	public final static String minCoords = "AGG_MIN_COORDINATES";
	public final static String tempAGG = "AGG_TEMP";
	public static final String correctedSizeAGG = "AGG_CORR_SIZE";
	protected final static String scaleFactorAgg = "AGG_SCALEFACTOR";
	public final static String componentNumber = "AGG_COMP_NUMBER";
	public final static String componentNoOfNodes = "AGG_COMPONENT_NO_OF_NODES";
	public static final String tempAggregator = "AGG_TEMP";
	protected static final String offsetsAggregator = "AGG_CC_BOXES";
	public static final String ttlMaxAggregator = "AGG_MAX_TTL";
	public static final String angleMaximizationClockwiseAggregator = "AGG_CLCKROTATION";
	public static final String initialTempFactorAggregator = "TEMP_FACT_AGG";
	public static final String coolingSpeedAggregator = "COOLING_SPEED_AGG";
	public static final String currentAccuracyAggregator = "CURRENT_ACCURACY_AGGREGATOR";

	//COUNTERS
	protected static final String COUNTER_GROUP = "Drawing Counters";

	protected static final String minRationThresholdString = "layout.minRatioThreshold";
	protected static final float defaultMinRatioThreshold = 0.2f;

	//GLOBAL STATIC VARIABLES
	public static boolean logLayout;

	//INSTANCE VARIABLES
	protected long propagationSteps;
	//	protected long allVertices;
	protected float threshold;
	protected boolean halting;
	long settledSteps;
	protected int readyToSleep;
	protected CoolingStrategy coolingStrategy;
	static int maxSuperstep;
	private boolean ignition;
	private long egira;
	private boolean firstCall;
	private int totalCalls;

	protected MasterCompute master;
	protected Class<? extends AbstractSeeder> seeder;
	protected Class<? extends AbstractPropagator> propagator;
	protected Class<? extends DrawingBoundariesExplorer> drawingExplorer;
	protected Class<? extends DrawingBoundariesExplorerWithComponentsNo> drawingExplorerWithCCs;
	protected Class<? extends DrawingScaler> drawingScaler;
	protected Class<? extends LayoutCCs> layoutCC;
	//	protected Class<? extends Computation> dummyComputation;

	public void initialize(MasterCompute myMaster
			, Class<? extends AbstractSeeder> seeder, Class<? extends AbstractPropagator> propagator,
			Class<? extends DrawingBoundariesExplorer> explorer, Class<? extends DrawingBoundariesExplorerWithComponentsNo> explorerWithCCs,
			Class<? extends DrawingScaler> drawingScaler,
			Class<? extends LayoutCCs> layoutCC)//,
			//			Class<? extends Computation> dummyComputation)
					throws InstantiationException,	IllegalAccessException {
		master = myMaster;
		this.seeder = seeder;
		this.propagator = propagator;
		drawingExplorer = explorer;
		drawingExplorerWithCCs = explorerWithCCs;
		this.layoutCC = layoutCC;
		this.drawingScaler = drawingScaler;

		ignition = true;
		firstCall = false;
		totalCalls = 0;
		
		logLayout = master.getConf().getBoolean(logLayoutString, false);

		maxSuperstep = master.getConf().getInt(computationLimit, maxSstepsDefault);

		threshold = master.getConf().getFloat(convergenceThresholdString, defaultConvergenceThreshold);

		master.registerAggregator(convergenceAggregatorString, LongSumAggregator.class);
		master.registerAggregator(MessagesAggregatorString, BooleanAndAggregator.class);

		master.registerPersistentAggregator(maxOneDegAggregatorString, IntMaxAggregator.class);

		settledSteps = 0;
		halting = false;

		// FRAME AGGREGATORS

		master.registerPersistentAggregator(correctedSizeAGG, ComponentMapOverwriteAggregator.class);

		// TEMP AGGREGATORS

		master.registerPersistentAggregator(tempAGG, ComponentMapOverwriteAggregator.class);

		// COORDINATES AGGREGATORS

		master.registerPersistentAggregator(maxCoords, ComponentFloatXYMaxAggregator.class);
		master.registerPersistentAggregator(minCoords, ComponentFloatXYMinAggregator.class);
		master.registerAggregator(scaleFactorAgg, ComponentMapOverwriteAggregator.class);

		// CONSTANT AGGREGATORS

		master.registerPersistentAggregator(max_K_agg, FloatMaxAggregator.class);
		master.registerPersistentAggregator(k_agg, FloatMaxAggregator.class);		
		master.registerPersistentAggregator(walshawConstant_agg, FloatMaxAggregator.class);
		master.registerPersistentAggregator(initialTempFactorAggregator, FloatMaxAggregator.class);
		master.registerPersistentAggregator(coolingSpeedAggregator, FloatMaxAggregator.class);
		master.registerPersistentAggregator(currentAccuracyAggregator, FloatMaxAggregator.class);
		
		//COMPONENT DATA AGGREGATORS

//		master.registerPersistentAggregator(componentNumber, IntSetAggregator.class);
		master.registerPersistentAggregator(componentNoOfNodes, ComponentIntSumAggregator.class);
		master.registerAggregator(offsetsAggregator, ComponentMapOverwriteAggregator.class);
		
		master.registerAggregator(angleMaximizationClockwiseAggregator, BooleanAndAggregator.class);

		//		float walshawModifier = master.getConf().getFloat(walshawModifierString, walshawModifierDefault);

		float nl = master.getConf().getFloat(LayoutRoutine.node_length , LayoutRoutine.defaultNodeValue);
		float nw = master.getConf().getFloat(LayoutRoutine.node_width ,LayoutRoutine.defaultNodeValue);
		float ns = master.getConf().getFloat(LayoutRoutine.node_separation ,LayoutRoutine.defaultNodeValue);
		float k = new Double(ns + Toolbox.computeModule(new float[]{nl, nw})).floatValue();
		master.setAggregatedValue(LayoutRoutine.k_agg, new FloatWritable(k));
		
//		coolingStrategy = new LinearCoolingStrategy(new String[]{master.getConf().get(LayoutRoutine.coolingSpeed, defaultCoolingSpeed )});
	}

	/**
	 * This method executes a number of tasks to tune the algorithm given the proportions of the initial (random) layout of each component.
	 * 
	 * @throws IllegalAccessException
	 */
	protected void superstepOneSpecials(float optimalEdgeLength) throws IllegalAccessException{

		MapWritable aggregatedMaxComponentData = master.getAggregatedValue(maxCoords);
		MapWritable aggregatedMinComponentData = master.getAggregatedValue(minCoords);
		MapWritable componentNodesMap = master.getAggregatedValue(componentNoOfNodes);

		Iterator<Entry<Writable, Writable>> iteratorOverComponents = aggregatedMaxComponentData.entrySet().iterator();

//		float tempConstant = master.getConf().getFloat(initialTempFactorString, defaultInitialTempFactor);
		
		coolingStrategy = new LinearCoolingStrategy(new String[]{String.valueOf(((FloatWritable)master.getAggregatedValue(coolingSpeedAggregator)).get())});
		
		float tempConstant = ((FloatWritable)master.getAggregatedValue(initialTempFactorAggregator)).get();
		
		MapWritable correctedSizeMap = new MapWritable();
		MapWritable tempMap = new MapWritable();
		MapWritable scaleFactorMap = new MapWritable();

		while(iteratorOverComponents.hasNext()){
			Entry<Writable, Writable> currentEntryMax = iteratorOverComponents.next();

			Writable key = currentEntryMax.getKey();

			float[] maxCurrent = ((FloatWritableArray)currentEntryMax.getValue()).get();
			float[] minCurrent = ((FloatWritableArray)aggregatedMinComponentData.get(key)).get();

			int noOfNodes = ((IntWritable)componentNodesMap.get(key)).get();
			if(noOfNodes == 1){
				float[] correctedSizes = new float[]{1, 1};
				float[] scaleFactors = new float[]{1, 1};
				float[] temps = new float[]{0, 0};

				correctedSizeMap.put(key, new FloatWritableArray(correctedSizes));
				tempMap.put(key, new FloatWritableArray(temps));
				scaleFactorMap.put(key, new FloatWritableArray(scaleFactors));
				continue;
			}
			
			float w = Toolbox.floatFuzzyMath((maxCurrent[0] - minCurrent[0])) + optimalEdgeLength;
			float h = Toolbox.floatFuzzyMath((maxCurrent[1] - minCurrent[1])) + optimalEdgeLength;

			float ratio = h/w;
			float W = new Double(Math.sqrt(noOfNodes/ratio)*optimalEdgeLength).floatValue();	
			float H = ratio*W;

			float[] correctedSizes = new float[]{W, H};
			float[] scaleFactors = new float[]{W/w, H/h};
			float[] temps = new float[]{W/tempConstant, H/tempConstant};

			correctedSizeMap.put(key, new FloatWritableArray(correctedSizes));
			tempMap.put(key, new FloatWritableArray(temps));
			scaleFactorMap.put(key, new FloatWritableArray(scaleFactors));

		}

		master.setAggregatedValue(correctedSizeAGG, correctedSizeMap);
		master.setAggregatedValue(tempAGG, tempMap);
		master.setAggregatedValue(scaleFactorAgg, scaleFactorMap);
	}

	/**
	 * Convenience method to update the temperature aggregator each time a new seeding phase is performed.
	 */
	protected void updateTemperatureAggregator(){
		MapWritable tempMap = master.getAggregatedValue(tempAGG);
		Iterator<Entry<Writable, Writable>> tempsIterator = tempMap.entrySet().iterator();
		MapWritable newTempsMap = new MapWritable();

		while(tempsIterator.hasNext()){
			Entry<Writable, Writable> currentTemp = tempsIterator.next();
			float[] temps = ((FloatWritableArray)currentTemp.getValue()).get();
			newTempsMap.put(currentTemp.getKey(), new FloatWritableArray(new float[]{coolingStrategy.cool(temps[0]),
					coolingStrategy.cool(temps[1])}));

		}		
		master.setAggregatedValue(tempAGG, newTempsMap);
	}

	/**
	 * 
	 * The main master compute method. 
	 * 
	 */
	public boolean compute(long noOfVertices, float optimalEdgeLength){
		if(ignition){
			totalCalls++;
			egira = master.getSuperstep();
			ignition = false;
			master.setComputation(drawingExplorerWithCCs);
			return false;
		}
		long relativeSuperstep = master.getSuperstep() - egira;
		if(logLayout)
			log.info("Relative superstep " + relativeSuperstep);
		if(relativeSuperstep > 5 && (checkForConvergence(noOfVertices) || relativeSuperstep > LayoutRoutine.maxSuperstep)){
			ignition = true;
			master.getContext().getCounter(COUNTER_GROUP, "Call " + totalCalls + "Drawing supersteps ").increment(master.getSuperstep() - egira);
			return true; //CHECK IF THE HALTING SEQUENCE IS IN PROGRESS
		}
		if(relativeSuperstep == 1){
			try {
//				log.info("iopti " + optimalEdgeLength);
				superstepOneSpecials(optimalEdgeLength); //COMPUTE THE FACTORS TO PREPARE THE GRAPH FOR THE LAYOUT.
				if(!firstCall){
					firstCall = true;
					master.setComputation(drawingScaler); //... AND APPLY THEM
				}else
					master.setComputation(seeder);
				return false;
			} catch (IllegalAccessException e) {
				master.haltComputation();
				return true;
			}
		}
		//REGIME COMPUTATION
		if(((BooleanWritable)master.getAggregatedValue(MessagesAggregatorString)).get() && !(master.getComputation().toString().contains("Seeder"))){
			if(settledSteps > 0)
				updateTemperatureAggregator();	//COOL DOWN THE TEMPERATURE
			master.setComputation(seeder); //PERFORM THE LAYOUT UPDATE AND SEEDING
			settledSteps++;
		}else
			if(!(master.getComputation().toString().contains("Propagator"))){
				master.setComputation(propagator); //PROPAGATE THE MESSAGES AND COMPUTE THE FORCES
			}	
		return false;
	}

	/**
	 * Check for graph equilibrium.
	 * @return true if the number of vertices which did not move above the threshold is higher than the convergence
	 * threshold.
	 */
	protected boolean checkForConvergence(){
		//		if(allVertices <= 0){
		//			allVertices = master.getTotalNumVertices();
		//			return false;
		//		}
		return ((LongWritable)master.getAggregatedValue(convergenceAggregatorString)).get()/master.getTotalNumVertices() > threshold;
	}

	/**
	 * Check for graph equilibrium.
	 * @return true if the number of vertices which did not move above the threshold is higher than the convergence
	 * threshold.
	 */
	protected boolean checkForConvergence(long subsetOfVertices){
		if(logLayout)
			log.info("The convergence is " + ((LongWritable)master.getAggregatedValue(convergenceAggregatorString)).get()/subsetOfVertices + " and da thres " + threshold);
		return ((LongWritable)master.getAggregatedValue(convergenceAggregatorString)).get()/subsetOfVertices > threshold;
	}



	/**
	 * In this computation each vertex simply aggregates its coordinates to the max and min coodinates aggregator of its component.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class DrawingBoundariesExplorer<V extends CoordinateWritable, E extends IntWritable>//, M1 extends LayoutMessageMatrix<I>, M2 extends LayoutMessageMatrix<I>> 
	extends AbstractComputation<LayeredPartitionedLongWritable, V, E, LayoutMessage, LayoutMessage> {
		
		protected float[] coords;
		protected V vValue;
		
		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
		 */
		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, V, E> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, V, E> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
		}

		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, V, E> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			vValue = vertex.getValue();
			coords = vValue.getCoordinates();
			MapWritable myCoordsPackage = new MapWritable();
			myCoordsPackage.put(new IntWritable(vValue.getComponent()), new FloatWritableArray(coords));
			aggregate(maxCoords, myCoordsPackage);
			aggregate(minCoords, myCoordsPackage);
		}

	}

	public static class DrawingBoundariesExplorerWithComponentsNo<V extends CoordinateWritable, E extends IntWritable>//>, M1 extends LayoutMessageMatrix<I>, M2 extends LayoutMessageMatrix<I>> extends
	extends DrawingBoundariesExplorer<V, E>{

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
		 */
		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, V, E> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, V, E> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
		}

		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, V, E> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			super.compute(vertex, msgs);
			MapWritable information = new MapWritable();
			information.put(new IntWritable(vValue.getComponent()), 
					new IntWritable((int)1 + vertex.getValue().getOneDegreeVerticesQuantity()));
			aggregate(componentNoOfNodes, information);
//			aggregate(componentNumber, new IntWritable(vValue.getComponent()));
		}
	}

	/**
	 * This computation applies a previously computed transformation stored into an aggregator (scaling+translation) to components' vertices.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class DrawingScaler <V extends CoordinateWritable, E extends IntWritable> extends
	AbstractComputation<LayeredPartitionedLongWritable, V, E,LayoutMessage, LayoutMessage>{

		MapWritable scaleFactors;
		MapWritable minCoordinateMap;

		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
		 */
		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, V, E> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, V, E> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
		}
		
		@Override
		public void preSuperstep() {
			super.preSuperstep();
			scaleFactors = getAggregatedValue(scaleFactorAgg);
			minCoordinateMap = getAggregatedValue(minCoords);
		}

		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, V, E> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			V vValue = vertex.getValue();
			float[] coords = vValue.getCoordinates();
			float[] factors = ((FloatWritableArray)scaleFactors.get(new IntWritable(vValue.getComponent()))).get();
			float[] minCoords = ((FloatWritableArray)minCoordinateMap.get(new IntWritable(vValue.getComponent()))).get();			
			vValue.setCoordinates((coords[0] - minCoords[0])*factors[0], (coords[1] - minCoords[1])*factors[1]);
		}
	}

	/**
	 * Given the scaling and traslating data computed to arrange the connected components, this computation applies them to each vertex.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class LayoutCCs <V extends CoordinateWritable, E extends IntWritable> extends
	AbstractComputation<LayeredPartitionedLongWritable, V, E, LayoutMessage, LayoutMessage>{

		MapWritable offsets;

		float componentPadding;
		
		/* (non-Javadoc)
		 * @see org.apache.giraph.graph.AbstractComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
		 */
		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, V, E> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, V, E> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
		}

		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, V, E> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			V vValue = vertex.getValue();
			float[] coords = vValue.getCoordinates();
			float[] ccOffset = ((FloatWritableArray)offsets.get(new IntWritable(vValue.getComponent()))).get();
			vValue.setCoordinates(((coords[0] + ccOffset[0])*ccOffset[2]) + ccOffset[3], ((coords[1] + ccOffset[1])*ccOffset[2]) + ccOffset[4]);
		}

		@Override
		public void preSuperstep() {
			offsets = getAggregatedValue(offsetsAggregator);
		}

	}

}
