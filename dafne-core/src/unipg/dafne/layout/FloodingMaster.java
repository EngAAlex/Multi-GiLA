package unipg.dafne.layout;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.FloatMaxAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import unipg.dafne.aggregators.ComponentAggregatorAbstract.ComponentFloatXYMaxAggregator;
import unipg.dafne.aggregators.ComponentAggregatorAbstract.ComponentFloatXYMinAggregator;
import unipg.dafne.aggregators.ComponentAggregatorAbstract.ComponentIntSumAggregator;
import unipg.dafne.aggregators.ComponentAggregatorAbstract.ComponentMapOverwriteAggregator;
import unipg.dafne.aggregators.SetAggregator;
import unipg.dafne.common.coordinatewritables.CoordinateWritable;
import unipg.dafne.common.datastructures.FloatWritableArray;
import unipg.dafne.common.datastructures.PartitionedLongWritable;
import unipg.dafne.common.datastructures.messagetypes.LayoutMessage;
import unipg.dafne.coolingstrategies.CoolingStrategy;
import unipg.dafne.coolingstrategies.LinearCoolingStrategy;
import unipg.dafne.layout.GraphReintegration.FairShareReintegrateOneEdges;
import unipg.dafne.layout.GraphReintegration.PlainDummyComputation;
import unipg.dafne.utils.Toolbox;

import com.google.common.collect.Lists;

/**
 * This class defines the behaviour of the layout phase of the algorithm, loading the appropriate computations at the right time. It defines
 * the stopping conditions, changes between the seeding and propagating phases and finally reintegrate the one-degree vertices before 
 * halting the computation.
 * 
 * @author general
 *
 */
public class FloodingMaster extends DefaultMasterCompute {
		
	//#############CLINT OPTIONS
	
	//COMPUTATION OPTIONS
	public static final String ttlMaxString = "layout.flooding.ttlMax";
	public static final String computationLimit = "layout.limit";
	public static final String convergenceThresholdString = "layout.convergence-threshold";
	public static final int ttlMaxDefault = 3;
	public static final int maxSstepsDefault = 1000;
	public static final float defaultConvergenceThreshold = 0.85f;

	//MESSAGES OPTIONS
	public static final String useQueuesString = "flooding.useQueues";
	public static final String queuePercentageString = "layout.queuepercentage";
	public static final float queuePercentageDefault = 0.1f;

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
	public float defaultInitialTempFactor = 0.4f;
	public final String defaultCoolingSpeed = "0.93";
	public static final float accuracyDefault = 0.01f;
	public static final String forceMethodOptionString = "layout.forceModel";
	public static final String useCosSinInForceComputation = "layout.useCosSin";
	public static final String sendDegTooOptionString = "layout.sendDegreeIntoLayoutMessages";
	
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
	static final String walshawConstant_agg = "WALSHAW_AGG";
	private final static String maxCoords = "AGG_MAX_COORDINATES";
	private final static String minCoords = "AGG_MIN_COORDINATES";
	public final static String tempAGG = "AGG_TEMP";
	public static final String correctedSizeAGG = "AGG_CORR_SIZE";
	protected final static String scaleFactorAgg = "AGG_SCALEFACTOR";
	protected final static String componentNumber = "AGG_COMP_NUMBER";
	protected final static String componentNoOfNodes = "AGG_COMPONENT_NO_OF_NODES";
	public static final String tempAggregator = "AGG_TEMP";
	protected static final String offsetsAggregator = "AGG_CC_BOXES";
	
	//COUNTERS
	protected static final String COUNTER_GROUP = "Drawing Counters";
	
	private static String minRationThresholdString = "layout.minRatioThreshold";
	private float defaultMinRatioThreshold = 0.2f;

	//VARIABLES
	protected long propagationSteps;
	protected long allVertices;
	protected float threshold;
	protected boolean halting;
	long settledSteps;
	protected int readyToSleep;
	protected CoolingStrategy coolingStrategy;
	static int maxSuperstep;
	
	@Override
	public void initialize() throws InstantiationException,
	IllegalAccessException {		
		maxSuperstep = getConf().getInt(computationLimit, maxSstepsDefault);

		threshold = getConf().getFloat(convergenceThresholdString, defaultConvergenceThreshold);

		registerAggregator(convergenceAggregatorString, LongSumAggregator.class);
		registerAggregator(MessagesAggregatorString, BooleanAndAggregator.class);

		registerPersistentAggregator(maxOneDegAggregatorString, IntMaxAggregator.class);

		settledSteps = 0;
		halting = false;

		// FRAME AGGREGATORS

		registerPersistentAggregator(correctedSizeAGG, ComponentMapOverwriteAggregator.class);

		// TEMP AGGREGATORS

		registerPersistentAggregator(tempAGG, ComponentMapOverwriteAggregator.class);

		// COORDINATES AGGREGATORS

		registerPersistentAggregator(maxCoords, ComponentFloatXYMaxAggregator.class);
		registerPersistentAggregator(minCoords, ComponentFloatXYMinAggregator.class);
		registerAggregator(scaleFactorAgg, ComponentMapOverwriteAggregator.class);

		// CONSTANT AGGREGATORS

		registerPersistentAggregator(k_agg, FloatMaxAggregator.class);		
		registerPersistentAggregator(walshawConstant_agg, FloatMaxAggregator.class);	
		
		//COMPONENT DATA AGGREGATORS
		
		registerPersistentAggregator(componentNumber, SetAggregator.class);
		registerPersistentAggregator(componentNoOfNodes, ComponentIntSumAggregator.class);
		registerAggregator(offsetsAggregator, ComponentMapOverwriteAggregator.class);

		float nl = getConf().getFloat(node_length ,defaultNodeValue);
		float nw = getConf().getFloat(node_width ,defaultNodeValue);
		float ns = getConf().getFloat(node_separation ,defaultNodeValue);
		float k = new Double(ns + Toolbox.computeModule(new float[]{nl, nw})).floatValue();
		setAggregatedValue(k_agg, new FloatWritable(k));
		
		float walshawModifier = getConf().getFloat(walshawModifierString, walshawModifierDefault);
		
		setAggregatedValue(walshawConstant_agg, new FloatWritable(new Double(Math.pow(k, 2) * walshawModifier).floatValue()));
		
		coolingStrategy = new LinearCoolingStrategy(new String[]{getConf().get(FloodingMaster.coolingSpeed, defaultCoolingSpeed )});
	}

	/**
	 * This method executes a number of tasks to tune the algorithm given the proportions of the initial (random) layout of each component.
	 * 
	 * @return
	 * @throws IllegalAccessException
	 */
	protected boolean superstepOneSpecials() throws IllegalAccessException{
		
		MapWritable aggregatedMaxComponentData = getAggregatedValue(maxCoords);
		MapWritable aggregatedMinComponentData = getAggregatedValue(minCoords);
		MapWritable componentNodesMap = getAggregatedValue(componentNoOfNodes);

		Iterator<Entry<Writable, Writable>> iteratorOverComponents = aggregatedMaxComponentData.entrySet().iterator();

		float k = ((FloatWritable)getAggregatedValue(k_agg)).get();
		float tempConstant = getConf().getFloat(initialTempFactorString, defaultInitialTempFactor);
		
		MapWritable correctedSizeMap = new MapWritable();
		MapWritable tempMap = new MapWritable();
		MapWritable scaleFactorMap = new MapWritable();

		while(iteratorOverComponents.hasNext()){
			Entry<Writable, Writable> currentEntryMax = iteratorOverComponents.next();
			
			Writable key = currentEntryMax.getKey();
			
			float[] maxCurrent = ((FloatWritableArray)currentEntryMax.getValue()).get();
			float[] minCurrent = ((FloatWritableArray)aggregatedMinComponentData.get(key)).get();
			
			int noOfNodes = ((IntWritable)componentNodesMap.get(key)).get();
			
			float w = (maxCurrent[0] - minCurrent[0]) + k;
			float h = (maxCurrent[1] - minCurrent[1]) + k;
						
			float ratio = h/w;
			float W = new Double(Math.sqrt(noOfNodes/ratio)*k).floatValue();	
			float H = ratio*W;

			float[] correctedSizes = new float[]{W, H};
			float[] scaleFactors = new float[]{W/w, H/h};
			float[] temps = new float[]{W/tempConstant, H/tempConstant};
			
			correctedSizeMap.put(key, new FloatWritableArray(correctedSizes));
			tempMap.put(key, new FloatWritableArray(temps));
			scaleFactorMap.put(key, new FloatWritableArray(scaleFactors));
			
		}
		
		setAggregatedValue(correctedSizeAGG, correctedSizeMap);
		setAggregatedValue(tempAGG, tempMap);
		setAggregatedValue(scaleFactorAgg, scaleFactorMap);

		return true;
	}

	/**
	 * Convenience method to update the temperature aggregator each time a new seeding phase is performed.
	 */
	protected void updateTemperatureAggregator(){
		MapWritable tempMap = getAggregatedValue(tempAGG);
		Iterator<Entry<Writable, Writable>> tempsIterator = tempMap.entrySet().iterator();
		MapWritable newTempsMap = new MapWritable();

		while(tempsIterator.hasNext()){
			Entry<Writable, Writable> currentTemp = tempsIterator.next();
			float[] temps = ((FloatWritableArray)currentTemp.getValue()).get();
			newTempsMap.put(currentTemp.getKey(), new FloatWritableArray(new float[]{coolingStrategy.cool(temps[0]),
																					 coolingStrategy.cool(temps[1])}));
			
		}		
		setAggregatedValue(tempAGG, newTempsMap);
	}
	
	/**
	 * The method is used to start the halting sequence and to manage the order of the events leading to the algorithm conclusion.
	 * 
	 * @throws IllegalAccessException
	 */
	protected void masterHaltingSequence(){
		if(readyToSleep != 0 || checkForConvergence()){ //IF TRUE, THE HALTING SEQUENCE IS IN PROGRESS
			halting = true;
			if(readyToSleep == 0){ //FIRST STEP: ONE DEGREE VERTICES REINTEGRATION
				try {
					setComputation((Class<? extends Computation>)Class.forName(getConf().get(oneDegreeReintegratingClassOption, FairShareReintegrateOneEdges.class.toString())));
				} catch (ClassNotFoundException e) {
					setComputation(FairShareReintegrateOneEdges.class);
				}	
				readyToSleep++;								
				return;
			}
			if(readyToSleep == 1){ //A BLANK COMPUTATION TO PROPAGATE THE GRAPH MODIFICATIONS MADE IN THE PREVIOUS SUPERSTEP
				setComputation(PlainDummyComputation.class);
				readyToSleep++;
				return;
			}
			if(readyToSleep == 2){ //SECOND STEP: TO COMPUTE THE FINAL GRID LAYOUT OF THE CONNECTED COMPONENTS, THEIR DRAWING
				setAggregatedValue(maxCoords, new MapWritable()); //PROPORTIONS ARE SCANNED.
				setAggregatedValue(minCoords, new MapWritable());
				setComputation(DrawingBoundariesExplorer.class);
				readyToSleep++;
				return;
			}
			if(readyToSleep == 3){ //THIRD STEP: ONCE THE DATA NEEDED TO LAYOUT THE CONNECTED COMPONENTS GRID ARE COMPUTED, 
				computeComponentGridLayout(); //THE LAYOUT IS COMPUTED.
				setComputation(LayoutCCs.class);			
				readyToSleep++;
				return;
			}
			
			haltComputation(); //THE SEQUENCE IS COMPLETED, THE COMPUTATION MAY NOW HALT.
		}
	}

	/**
	 * 
	 * The main master compute method. 
	 * 
	 */
	@Override
	public void compute(){
		if(getSuperstep() == 0){
			return;
		}
		
		masterHaltingSequence(); //CHECK IF THE HALTING SEQUENCE IS IN PROGRESS

		if(halting) //IF IT IS, THIS STEP MASTER COMPUTATION ENDS HERE.
			return;
		
		if(getSuperstep() == 1){
			try {
				if(superstepOneSpecials()){ //COMPUTE THE FACTORS TO PREPARE THE GRAPH FOR THE LAYOUT.
					setComputation(DrawingScaler.class); //... AND APPLY THEM
					return;
				}
			} catch (IllegalAccessException e) {
				haltComputation();
			}
		}		
		
		//REGIME COMPUTATION
		if(((BooleanWritable)getAggregatedValue(MessagesAggregatorString)).get() && !(getComputation().toString().contains("Seeder"))){
			if(settledSteps > 0)
				updateTemperatureAggregator();	//COOL DOWN THE TEMPERATURE
			setComputation(Seeder.class); //PERFORM THE LAYOUT UPDATE AND SEEDING
			settledSteps++;
		}else
			if(!(getComputation().toString().contains("Propagator"))){
				setComputation(Propagator.class); //PROPAGATE THE MESSAGES AND COMPUTE THE FORCES
			}	


	}

	/**
	 * Check for graph equilibrium.
	 * @return true if the number of vertices which did not move above the threshold is higher than the convergence
	 * threshold.
	 */
	protected boolean checkForConvergence(){
		if(allVertices <= 0){
			allVertices = getTotalNumVertices();
			return false;
		}
		return ((LongWritable)getAggregatedValue(convergenceAggregatorString)).get()/allVertices > threshold;
	}
	
	/**
	 * This method computes the connected components final grid layout.
	 */
	protected void computeComponentGridLayout() {
		
		float componentPadding = getConf().getFloat(FloodingMaster.componentPaddingConfString, defaultPadding);
		float minRatioThreshold = getConf().getFloat(FloodingMaster.minRationThresholdString, defaultMinRatioThreshold );
		
		MapWritable offsets = new MapWritable();
				
		MapWritable maxCoordsMap = getAggregatedValue(maxCoords);
		MapWritable minCoordsMap = getAggregatedValue(minCoords);
		MapWritable componentsNo = getAggregatedValue(componentNoOfNodes);
		
		//##### SORTER -- THE MAP CONTAINING THE COMPONENTS' SIZES IS SORTED BY ITS VALUES
		
		LinkedHashMap<LongWritable, IntWritable> sortedMap = (LinkedHashMap<LongWritable, IntWritable>)sortMapByValues(componentsNo);
	
		LongWritable[] componentSizeSorter = sortedMap.keySet().toArray(new LongWritable[0]);
		IntWritable[] componentSizeSorterValues = sortedMap.values().toArray(new IntWritable[0]);
				
		int coloumnNo = new Double(Math.ceil(Math.sqrt(componentsNo.size() - 1))).intValue();
		
		Point2D.Float cursor = new Point2D.Float(0.0f, 0.0f);
		Point2D.Float tableOrigin = new Point2D.Float(0.0f, 0.0f);
		
		Long maxID = componentSizeSorter[componentSizeSorter.length-1].get();
		int maxNo = componentSizeSorterValues[componentSizeSorter.length-1].get();// ((LongWritable)componentsNo.get(new LongWritable(maxID))).get();
		
		float[] translationCorrection = ((FloatWritableArray)minCoordsMap.get(new LongWritable(maxID))).get();
		offsets.put(new LongWritable(maxID), new FloatWritableArray(new float[]{-translationCorrection[0], -translationCorrection[1], 1.0f, cursor.x, cursor.y}));
		
		float[] maxComponents = ((FloatWritableArray)maxCoordsMap.get(new LongWritable(maxID))).get();
//		float componentPadding = getConf().getFloat(FloodingMaster.componentPaddingConfString, defaultPadding)*maxComponents[0];
		cursor.setLocation((maxComponents[0] - translationCorrection[0]) + componentPadding, 0.0f); //THE BIGGEST COMPONENT IS PLACED IN THE UPPER LEFT CORNER.
		tableOrigin.setLocation(cursor);
			
		float coloumnMaxY = 0.0f;
		int counter = 1;
				
		for(int j=componentSizeSorter.length-2; j>=0; j--){ //THE OTHER SMALLER COMPONENTS ARE ARRANGED IN A GRID.
			long currentComponent = componentSizeSorter[j].get();
			maxComponents = ((FloatWritableArray)maxCoordsMap.get(new LongWritable(currentComponent))).get();
			float sizeRatio = (float)componentSizeSorterValues[j].get()/maxNo;
			translationCorrection = ((FloatWritableArray)minCoordsMap.get(new LongWritable(currentComponent))).get();
						
			if(sizeRatio < minRatioThreshold)	
				sizeRatio = minRatioThreshold;
			
			maxComponents[0] -= translationCorrection[0];
			maxComponents[1] -= translationCorrection[1];
			maxComponents[0] *= sizeRatio;
			maxComponents[1] *= sizeRatio;
			
			offsets.put(new LongWritable(currentComponent), new FloatWritableArray(new float[]{-translationCorrection[0], -translationCorrection[1], sizeRatio,  cursor.x, cursor.y}));
			if(maxComponents[1] > coloumnMaxY)
				coloumnMaxY = maxComponents[1];
			if(counter % coloumnNo != 0){
				cursor.setLocation(cursor.x + maxComponents[0] + componentPadding, cursor.y);
				counter++;
			}else{
				cursor.setLocation(tableOrigin.x, cursor.y + coloumnMaxY + componentPadding);
				coloumnMaxY = 0.0f;
				counter = 1;
			}
		}
		setAggregatedValue(offsetsAggregator, offsets); //THE VALUES COMPUTED TO LAYOUT THE COMPONENTS ARE STORED INTO AN AGGREGATOR.
	}
	
	/**
	 * This method sorts a map by its values.
	 * 
	 * @param mapToSort
	 * @return The sorted LinkedHashMap.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static LinkedHashMap sortMapByValues(Map mapToSort){
		List keys = Lists.newArrayList(mapToSort.keySet());
		List values = Lists.newArrayList(mapToSort.values());
				
		Collections.sort(keys);
		Collections.sort(values);	
		
		LinkedHashMap sortedMap = new LinkedHashMap(mapToSort.size());
		
		Iterator vals = values.iterator();
		
		while(vals.hasNext()){
			Object currentVal = vals.next();
			Iterator keysToIterate = keys.iterator();
			
			while(keysToIterate.hasNext()){
				Object currentKey = keysToIterate.next();
				if(mapToSort.get(currentKey).equals(currentVal)){
					sortedMap.put(currentKey, currentVal);
					keys.remove(currentKey);
					break;
				}	
			}
		}
		
		return sortedMap;
		
	}

	/**
	 * In this computation each vertex simply aggregates its coordinates to the max and min coodinates aggregator of its component.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class DrawingBoundariesExplorer extends
	AbstractComputation<PartitionedLongWritable, CoordinateWritable, NullWritable, LayoutMessage, LayoutMessage> {

		protected float[] coords;
		protected CoordinateWritable vValue;
		
		@Override
		public void compute(
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			vValue = vertex.getValue();
			coords = vValue.getCoordinates();
			MapWritable myCoordsPackage = new MapWritable();
			myCoordsPackage.put(new LongWritable(vValue.getComponent()), new FloatWritableArray(coords));
			aggregate(maxCoords, myCoordsPackage);
			aggregate(minCoords, myCoordsPackage);
		}
		
		public static class DrawingBoundariesExplorerWithComponentsNo extends DrawingBoundariesExplorer{
			
			@Override
			public void compute(
					Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
					Iterable<LayoutMessage> msgs) throws IOException {
				super.compute(vertex, msgs);
				MapWritable information = new MapWritable();
				information.put(new LongWritable(vValue.getComponent()), 
						new IntWritable((int)1 + vertex.getValue().getOneDegreeVerticesQuantity()));
				aggregate(componentNoOfNodes, information);
				aggregate(componentNumber, new LongWritable(vValue.getComponent()));
				}
		}
	}

	/**
	 * This computation applies a previously computed transformation stored into an aggregator (scaling+translation) to components' vertices.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class DrawingScaler extends
	AbstractComputation<PartitionedLongWritable, CoordinateWritable, NullWritable, LayoutMessage, LayoutMessage> {
		
		MapWritable scaleFactors;
		MapWritable minCoordinateMap;

		@Override
		public void preSuperstep() {
			super.preSuperstep();
			scaleFactors = getAggregatedValue(scaleFactorAgg);
			minCoordinateMap = getAggregatedValue(minCoords);
		}

		@Override
		public void compute(
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
			CoordinateWritable vValue = vertex.getValue();
			float[] coords = vValue.getCoordinates();
			float[] factors = ((FloatWritableArray)scaleFactors.get(new LongWritable(vValue.getComponent()))).get();
			float[] minCoords = ((FloatWritableArray)minCoordinateMap.get(new LongWritable(vValue.getComponent()))).get();			
			vValue.setCoordinates((coords[0] - minCoords[0])*factors[0], (coords[1] - minCoords[1])*factors[1]);
			}
	}
	
	/**
	 * Given the scaling and traslating data computed to arrange the connected components, this computation applies them to each vertex.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class LayoutCCs extends
	AbstractComputation<PartitionedLongWritable, CoordinateWritable, NullWritable, LayoutMessage, LayoutMessage> {

		MapWritable offsets;
		
		float componentPadding;
		
		@Override
		public void compute(
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
				Iterable<LayoutMessage> msgs) throws IOException {
				CoordinateWritable vValue = vertex.getValue();
				float[] coords = vValue.getCoordinates();
				float[] ccOffset = ((FloatWritableArray)offsets.get(new LongWritable(vValue.getComponent()))).get();
				vValue.setCoordinates(((coords[0] + ccOffset[0])*ccOffset[2]) + ccOffset[3], ((coords[1] + ccOffset[1])*ccOffset[2]) + ccOffset[4]);
		}
	
		@Override
		public void preSuperstep() {
			offsets = getAggregatedValue(offsetsAggregator);
		}
		
	}

}
