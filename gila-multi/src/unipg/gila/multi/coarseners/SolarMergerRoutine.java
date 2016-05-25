/**
 * 
 */
package unipg.gila.multi.coarseners;

import java.util.Collection;
import java.util.Iterator;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.FloatMaxAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.aggregators.LongWritableSetAggregator;
import unipg.gila.aggregators.ComponentAggregatorAbstract.ComponentIntMaxAggregator;
import unipg.gila.aggregators.ComponentAggregatorAbstract.ComponentIntSumAggregator;
import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.LongWritableSet;
import unipg.gila.common.multi.SolarMessage;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.coarseners.SolarMerger.MoonSweep;
import unipg.gila.multi.coarseners.SolarMerger.PlanetResponse;
import unipg.gila.multi.coarseners.SolarMerger.AsteroidCaller;
import unipg.gila.multi.coarseners.SolarMerger.DummySolarMergerComputation;
import unipg.gila.multi.coarseners.SolarMerger.RegimeMerger;
import unipg.gila.multi.coarseners.SolarMerger.SolarMergeVertexCreation;
import unipg.gila.multi.coarseners.SolarMerger.SunDiscovery;
import unipg.gila.multi.coarseners.SolarMerger.SolarSweep;
import unipg.gila.multi.coarseners.SolarMerger.SunBroadcast;
import unipg.gila.multi.coarseners.SolarMerger.SunGeneration;
import unipg.gila.multi.layout.MultiScaleLayout;


/**
 * @author Alessio Arleo
 *
 */
public class SolarMergerRoutine {

	/*
	 * LOGGER 
	 * */
	protected static Logger log = Logger.getLogger(SolarMergerRoutine.class);

	public static final String currentLayer = "AGG_CURRENTLAYER";
	public static final String layerVertexSizeAggregator = "AGG_VERTEX_SIZE";
	public static final String layerEdgeSizeAggregator = "AGG_EDGE_SIZE";	
	public static final String layerEdgeWeightsAggregator = "AGG_EDGE_WEIGHTS";	
	public static final String layerNumberAggregator = "AGG_LAYER_NO";

	//MERGER AGGs
	public static final String messagesDepleted = "AGG_MSGSLEFT";
	public static final String asteroidsRemoved = "AGG_ASTEROIDSLEFT";
	public static final String mergerAttempts = "AGG_MERGERRUNS";

	//MERGER OPTIONS
	public static final String mergerConvergenceThreshold = "merger.convergenceThreshold";
	public static final int mergerConvergenceThresholdDefault = 10;
	public static final String sunChance = "merger.SunChance";
	public static final float sunChanceDefault = 0.2f;
	public static final String sunChanceAggregatorString = "AGG_SUNCHANCE";
	
	//MERGER COUNTERS
	public static final String COUNTER_GROUP = "Merging Counters";
	private static final String NUMBER_OF_LEVELS_COUNTER = "Number of levels";
	public static final String logMergerString = "merger.showLog";

	public static final String sunsPerComponent = "AGG_SUNS_PER_COMPONENT";

	//INSTANCE ATTRIBUTES
	int layerThreshold;
	boolean checkForNewLayer = false;
	boolean waitForDummy = false;
	boolean timeForTheMoons = false;
	boolean terminate = false;
	int attempts = 0;
	float baseSunChance;
	
	MasterCompute master;

	public boolean compute() {
		if(waitForDummy){
			waitForDummy = false;
			checkForNewLayer = true;
			master.setComputation(DummySolarMergerComputation.class);
			return false;
		}
		
		if(master.getSuperstep() == 0){
			return false;
		}
		
//		if(master.getComputation().equals(MultiScaleLayout.MultiScaleGraphExplorerWithComponentsNo.class)){
//			master.setComputation(SunGeneration.class);
//			return false;
//		}
		
		if(master.getSuperstep() == 1){
			MapWritable setupInfoV = new MapWritable();
			MapWritable setupInfoE = new MapWritable();		
			MapWritable setupInfoW = new MapWritable();
			
			setupInfoV.put(new IntWritable(0), new IntWritable((int)master.getTotalNumVertices()));
			master.setAggregatedValue(layerVertexSizeAggregator, setupInfoV);
			setupInfoE.put(new IntWritable(0), new IntWritable((int)master.getTotalNumEdges()));
			master.setAggregatedValue(layerEdgeSizeAggregator, setupInfoE);
//			setupInfoW.put(new IntWritable(0), new IntWritable((int)master.getTotalNumEdges()));
			setupInfoW.put(new IntWritable(0), new IntWritable(1));
			
			master.setAggregatedValue(layerEdgeWeightsAggregator, setupInfoW);
		}
		
		boolean messagesNegotiationDone = ((BooleanWritable)master.getAggregatedValue(messagesDepleted)).get();
		boolean asteroidsAssigned = ((BooleanWritable)master.getAggregatedValue(asteroidsRemoved)).get();
		
		if(master.getComputation().equals(SunGeneration.class)){
			master.setComputation(SolarSweep.class);
			return false;
		}
		if(master.getComputation().equals(SolarSweep.class)){
			if(messagesNegotiationDone)
				master.setComputation(SunBroadcast.class);
			return false;
		}
		if(master.getComputation().equals(SunBroadcast.class)){
			master.setComputation(PlanetResponse.class);
			return false;
		}
		if(master.getComputation().equals(PlanetResponse.class)){
			master.setComputation(RegimeMerger.class);
			return false;
		}
		if(messagesNegotiationDone && master.getComputation().equals(RegimeMerger.class)){
			if(!timeForTheMoons)
				master.setComputation(AsteroidCaller.class);
			else{
				master.setComputation(SolarMergeVertexCreation.class);
				//				creatingNewLayerVertices = false;
				waitForDummy = true;
				timeForTheMoons = false;
			}
			return false;
		}
		if(master.getComputation().equals(AsteroidCaller.class)){
			if(asteroidsAssigned){
				master.setComputation(MoonSweep.class);
				timeForTheMoons = true;
			}else{
				master.setComputation(SunGeneration.class);
				float currentSunChance = ((FloatWritable)master.getAggregatedValue(sunChanceAggregatorString)).get();
				currentSunChance += currentSunChance*0.5;
				master.setAggregatedValue(sunChanceAggregatorString, new FloatWritable(currentSunChance));
			}
			return false;
		}		
		if(master.getComputation().equals(MoonSweep.class)){
			master.setComputation(SunDiscovery.class);
			return false;
		}
		if(master.getComputation().equals(SunDiscovery.class)){
			master.setComputation(RegimeMerger.class);
			return false;
		}
//		if(getComputation().equals(EdgeDuplicatesRemover.class)){
//			setComputation(SunGeneration.class);
//			return;
//		}
		int cLayer = ((IntWritable)master.getAggregatedValue(currentLayer)).get();
		if(checkForNewLayer){
			checkForNewLayer = false;
			waitForDummy = false;
			int layerSize = ((IntWritable)((MapWritable)master.getAggregatedValue(layerVertexSizeAggregator)).get(new IntWritable(cLayer+1))).get();
			int currentLayerNo = ((IntWritable)master.getAggregatedValue(layerNumberAggregator)).get();
			master.setAggregatedValue(layerNumberAggregator, new IntWritable(currentLayerNo++));
			master.getContext().getCounter(COUNTER_GROUP, NUMBER_OF_LEVELS_COUNTER).increment(1);
			if(master.getSuperstep() > 1){
				master.setAggregatedValue(currentLayer, new IntWritable(cLayer+1));
				int avgNoOfSuns = computeAverageOfValueSet(((MapWritable)master.getAggregatedValue(sunsPerComponent)).values());
				log.info("GIGI avh" + layerSize + " " + avgNoOfSuns);
				if(avgNoOfSuns <= layerThreshold){
					//haltComputation();
					return true;
				}else{
					master.setComputation(SunGeneration.class);
//					master.setComputation(MultiScaleLayout.MultiScaleGraphExplorerWithComponentsNo.class);
					master.setAggregatedValue(sunsPerComponent, new MapWritable());
					master.setAggregatedValue(sunChanceAggregatorString, new FloatWritable(master.getConf().getFloat(sunChance, sunChanceDefault)));
				}
			}
//			setComputation(EdgeDuplicatesRemover.class);
			return false;
		}
		return false;
	}

	/**
	 * @param values
	 * @return
	 */
	private int computeAverageOfValueSet(Collection<Writable> values) {
		Iterator<Writable> it = values.iterator();
		int sum = 0;
		while(it.hasNext()){
			sum += ((IntWritable)it.next()).get();
		}
		return sum/values.size();
	}




	public void initialize(MasterCompute myMaster) throws InstantiationException,
	IllegalAccessException {
		master = myMaster;
		master.registerPersistentAggregator(currentLayer, IntMaxAggregator.class);
		master.registerPersistentAggregator(layerNumberAggregator, IntSumAggregator.class);
		master.registerPersistentAggregator(layerVertexSizeAggregator, ComponentIntSumAggregator.class);
		master.registerPersistentAggregator(layerEdgeSizeAggregator, ComponentIntSumAggregator.class);		
		master.registerPersistentAggregator(layerEdgeWeightsAggregator, ComponentIntMaxAggregator.class);		
		master.registerPersistentAggregator(mergerAttempts, IntMaxAggregator.class);
		master.registerAggregator(asteroidsRemoved, BooleanAndAggregator.class);
		master.registerAggregator(messagesDepleted, BooleanAndAggregator.class);
		master.setAggregatedValue(currentLayer, new IntWritable(0));
		master.registerPersistentAggregator(sunChanceAggregatorString, FloatMaxAggregator.class);
		
		master.setAggregatedValue(layerNumberAggregator, new IntWritable(0));
		master.setAggregatedValue(mergerAttempts, new IntWritable(1));
		master.setAggregatedValue(sunChanceAggregatorString, new FloatWritable(master.getConf().getFloat(sunChance, sunChanceDefault)));
		
		master.registerPersistentAggregator(sunsPerComponent, ComponentIntSumAggregator.class);
		
		layerThreshold = master.getConf().getInt(mergerConvergenceThreshold, mergerConvergenceThresholdDefault);
		
		SolarMerger.logMerger = master.getConf().getBoolean(logMergerString, false);
	}

}



