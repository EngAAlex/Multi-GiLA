/**
 * 
 */
package unipg.gila.multi;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.FloatMaxAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import unipg.gila.aggregators.ComponentAggregatorAbstract.ComponentIntSumAggregator;
import unipg.gila.multi.coarseners.SolarMerger.PlanetResponse;
import unipg.gila.multi.coarseners.SolarMerger.SolarSweep;
import unipg.gila.multi.coarseners.SolarMerger.SunBroadcast;
import unipg.gila.multi.coarseners.SolarMerger.SunGeneration;
import unipg.gila.multi.coarseners.SolarMerger.PlanetResponse.AsteroidCaller;
import unipg.gila.multi.coarseners.SolarMerger.PlanetResponse.DummySolarMergerComputation;
import unipg.gila.multi.coarseners.SolarMerger.PlanetResponse.MoonSweep;
import unipg.gila.multi.coarseners.SolarMerger.PlanetResponse.RegimeMerger;
import unipg.gila.multi.coarseners.SolarMerger.PlanetResponse.SolarMergeVertexCreation;
import unipg.gila.multi.coarseners.SolarMerger.PlanetResponse.SunDiscovery;


/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleDirector extends DefaultMasterCompute {

	/*
	 * LOGGER 
	 * */

	protected static Logger log = Logger.getLogger(MultiScaleDirector.class);

	public static final String currentLayer = "AGG_CURRENTLAYER";
	public static final String layerSizeAggregator = "AGG_LAYER_SIZE";
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
	
	//PLACER OPTIONS
	public static final String placerDefaultLengthString = "coarsener.defaultLength";
	public static final float placerDefaultLength = 15.0f;

	//INSTANCE ATTRIBUTES
	boolean creatingNewLayerVertices = false;
	boolean checkForNewLayer = false;
	boolean waitForDummy = false;
	boolean timeForTheMoons = false;
	boolean terminate = false;
	int attempts = 0;
	float baseSunChance;

	@Override
	public void compute() {
		if(terminate){
			log.info("FINALRESULT LAST LAYER " + getAggregatedValue(currentLayer));
			haltComputation();
			return;
		}
		if(waitForDummy){
			waitForDummy = false;
			checkForNewLayer = true;
			setComputation(DummySolarMergerComputation.class);
			return;
		}
		if(getSuperstep() == 0){
			MapWritable setupInfo = new MapWritable();
			//				setAggregatedValue(currentLayer, new IntWritable(0));
			setAggregatedValue(layerSizeAggregator, setupInfo.put(new IntWritable(0), new IntWritable((int)getTotalNumVertices())));
			setAggregatedValue(mergerAttempts, new IntWritable(1));
			setAggregatedValue(sunChanceAggregatorString, new FloatWritable(getConf().getFloat(sunChance, sunChanceDefault)));
			return;
		}
		boolean messagesNegotiationDone = ((BooleanWritable)getAggregatedValue(messagesDepleted)).get();
		boolean asteroidsAssigned = ((BooleanWritable)getAggregatedValue(asteroidsRemoved)).get();
		if(getComputation().equals(SunGeneration.class)){
			setComputation(SolarSweep.class);
			return;
		}
		if(getComputation().equals(SolarSweep.class)){
			if(messagesNegotiationDone)
				setComputation(SunBroadcast.class);
			return;
		}
		if(getComputation().equals(SunBroadcast.class)){
			setComputation(PlanetResponse.class);
			return;
		}
		if(getComputation().equals(PlanetResponse.class)){
			setComputation(RegimeMerger.class);
			return;
		}
		if(messagesNegotiationDone && getComputation().equals(RegimeMerger.class)){
			if(!timeForTheMoons)
				setComputation(AsteroidCaller.class);
			else{
				setComputation(SolarMergeVertexCreation.class);
				//				creatingNewLayerVertices = false;
				creatingNewLayerVertices = true;
				waitForDummy = true;
				timeForTheMoons = false;
			}
			return;
		}
		if(getComputation().equals(AsteroidCaller.class)){
			if(asteroidsAssigned){
				setComputation(MoonSweep.class);
				timeForTheMoons = true;
			}else{
				setComputation(SunGeneration.class);
				float currentSunChance = ((FloatWritable)getAggregatedValue(sunChanceAggregatorString)).get();
				currentSunChance += currentSunChance*0.5;
				setAggregatedValue(sunChanceAggregatorString, new FloatWritable(currentSunChance));
			}
			return;
		}		
		if(getComputation().equals(MoonSweep.class)){
			setComputation(SunDiscovery.class);
			return;
		}
		if(getComputation().equals(SunDiscovery.class)){
			setComputation(RegimeMerger.class);
			return;
		}
//		if(getComputation().equals(EdgeDuplicatesRemover.class)){
//			setComputation(SunGeneration.class);
//			return;
//		}
		int cLayer = ((IntWritable)getAggregatedValue(currentLayer)).get();
		if(checkForNewLayer){
			checkForNewLayer = false;
			waitForDummy = false;
			creatingNewLayerVertices = false;
			MapWritable mp = (MapWritable)getAggregatedValue(layerSizeAggregator);
			int layerSize = ((IntWritable)mp.get(new IntWritable(cLayer+1))).get();
			if(getSuperstep() > 1){
				setAggregatedValue(currentLayer, new IntWritable(cLayer+1));
				if(layerSize <= getConf().getInt(mergerConvergenceThreshold, mergerConvergenceThresholdDefault)){
					terminate = true;
				}else{
//					if(layerSize != null)
//						if(layerSize.get() == ((IntWritable)mp.get(new IntWritable(cLayer))).get()){ ###THERE IS STILL THE RISK OF SAME-SIZE LAYERS
							mp.put(new IntWritable(cLayer+1), new IntWritable(0));
							setAggregatedValue(layerSizeAggregator, mp);
//						}else
						setComputation(SunGeneration.class);
						setAggregatedValue(sunChanceAggregatorString, new FloatWritable(getConf().getFloat(sunChance, sunChanceDefault)));
				}
			}
//			setComputation(EdgeDuplicatesRemover.class);
			return;
		}
	}


	@Override
	public void initialize() throws InstantiationException,
	IllegalAccessException {
		registerPersistentAggregator(currentLayer, IntMaxAggregator.class);
		registerPersistentAggregator(layerNumberAggregator, IntSumAggregator.class);
		registerPersistentAggregator(layerSizeAggregator, ComponentIntSumAggregator.class);
		registerPersistentAggregator(mergerAttempts, IntMaxAggregator.class);
		registerAggregator(asteroidsRemoved, BooleanAndAggregator.class);
		registerAggregator(messagesDepleted, BooleanAndAggregator.class);
		setAggregatedValue(currentLayer, new IntWritable(0));
		registerPersistentAggregator(sunChanceAggregatorString, FloatMaxAggregator.class);
	}

}



