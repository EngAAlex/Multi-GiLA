/**
 * 
 */
package unipg.gila.multi;

import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.MergerToPlacerDummyComputation;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.layout.AdaptationStrategy;
import unipg.gila.multi.layout.LayoutAdaptationStrategy.SizeAndDensityDrivenAdaptationStrategy;
import unipg.gila.multi.layout.MultiScaleLayout;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleDrawingScaler;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleGraphExplorer;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleGraphExplorerWithComponentsNo;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleLayoutCC;
import unipg.gila.multi.placers.SolarPlacerRoutine;

/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleMaster extends DefaultMasterCompute {

	//LOGGER
	Logger log = Logger.getLogger(getClass());

	public static final String adaptationStrategyString = "multi.layout.adaptationStrategy";
	public static final String angularMaximizationIterationsString = "multi.layout.angularMaximizationMaxIterations";
	public static final int angularMaximizationMaxIterationsDefault = 30;	

	public static final String multiCounterString = "Global Counters";

	LayoutRoutine layoutRoutine;
	SolarMergerRoutine mergerRoutine;
	SolarPlacerRoutine placerRoutine;
	GraphReintegrationRoutine reintegrationRoutine;
	AdaptationStrategy adaptationStrategy;

	boolean merging;
	boolean placing;
	boolean layout;
	boolean reintegrating;
	boolean preparePlacer;
	boolean angularMaximization;
	boolean forceMaximization;
	int angularMaximizationIterations;
	int angularMaximizationIterationsMax;
	boolean terminate;

	@SuppressWarnings("unchecked")
	public void initialize() throws InstantiationException ,IllegalAccessException {
		layoutRoutine = new LayoutRoutine();
		layoutRoutine.initialize(this, MultiScaleLayout.Seeder.class, MultiScaleLayout.Propagator.class,
				MultiScaleGraphExplorer.class, MultiScaleGraphExplorerWithComponentsNo.class,
				MultiScaleDrawingScaler.class,
				MultiScaleLayoutCC.class);

		mergerRoutine = new SolarMergerRoutine();
		mergerRoutine.initialize(this);

		placerRoutine = new SolarPlacerRoutine();
		placerRoutine.initialize(this);

		reintegrationRoutine = new GraphReintegrationRoutine();
		reintegrationRoutine.initialize(this);

		merging=false;
		layout=false;
		reintegrating=false;
		preparePlacer = false;
		placing=false;
		terminate = false;
		angularMaximization = false;
		angularMaximizationIterations = 0;
		angularMaximizationIterationsMax = getConf().getInt(angularMaximizationIterationsString, angularMaximizationMaxIterationsDefault);

		try {
			Class<? extends AdaptationStrategy> tClass = (Class<? extends AdaptationStrategy>) Class.forName(getConf().getStrings(adaptationStrategyString, SizeAndDensityDrivenAdaptationStrategy.class.toString())[0]);
			adaptationStrategy = tClass.getConstructor().newInstance();
		} catch (Exception e) {
			log.info("Caught exceptione, sweithcin to default");
			adaptationStrategy = new SizeAndDensityDrivenAdaptationStrategy();
		} 

		registerPersistentAggregator(LayoutRoutine.ttlMaxAggregator, IntMaxAggregator.class);

	}

	public void compute() {
		if(getSuperstep() == 0){
			merging = true;
		}
		if(terminate){
			getContext().getCounter(multiCounterString, "Supersteps").increment(getSuperstep());
			haltComputation();
			return;
		}
		int currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get(); 

		if(merging)
			if(!mergerRoutine.compute()){
				return;
			}
			else{
				merging = false;
				preparePlacer = true;
				setComputation(MergerToPlacerDummyComputation.class);
				return;
			}
		int noOfVertices = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerVertexSizeAggregator)).get(new IntWritable(currentLayer))).get();
		int noOfLayers = ((IntWritable)getAggregatedValue(SolarMergerRoutine.layerNumberAggregator)).get();
		int noOfEdges = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator)).get(new IntWritable(currentLayer))).get();

		if(preparePlacer){
			preparePlacer = false;
			layout = true;
			//			MapWritable mpV = ((MapWritable)getAggregatedValue(SolarMergerRoutine.layerVertexSizeAggregator));
			//			MapWritable mpE = ((MapWritable)getAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator));
			//
			////			log.info("Current layer " + currentLayer);
			////
			////			log.info("Vertices map ");
			////			Iterator<Entry<Writable, Writable>> itV = mpV.entrySet().iterator();
			////			while(itV.hasNext()){
			////				Entry<Writable, Writable> current = itV.next();
			////				log.info(current.getKey() + " - " + current.getValue());
			////			}
			////			log.info("Edges map ");
			////			Iterator<Entry<Writable, Writable>> itE = mpE.entrySet().iterator();
			////			while(itE.hasNext()){
			////				Entry<Writable, Writable> current = itE.next();
			////				log.info(current.getKey() + " - " + current.getValue());
			////			}

			int selectedK = adaptationStrategy.returnCurrentK(currentLayer, noOfLayers, 
					noOfVertices, 
					noOfEdges);
			if(currentLayer == 0)
				selectedK = (selectedK > 2 ? 2 : selectedK);
			setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(selectedK));

			setAggregatedValue(LayoutRoutine.coolingSpeedAggregator, new FloatWritable(
					adaptationStrategy.returnCurrentCoolingSpeed(currentLayer, noOfLayers, noOfVertices, noOfEdges)));
			setAggregatedValue(LayoutRoutine.initialTempFactorAggregator, new FloatWritable(
					adaptationStrategy.returnCurrentInitialTempFactor(currentLayer, noOfLayers, noOfVertices, noOfEdges)));
			setAggregatedValue(LayoutRoutine.currentAccuracyAggregator, new FloatWritable(
					adaptationStrategy.returnTargetAccuracyy(currentLayer, noOfLayers, noOfVertices, noOfEdges)));

			getContext().getCounter("Layer Counters", "Layer " + currentLayer + " k").increment(selectedK);
			getContext().getCounter("Layer Counters", "Layer " + currentLayer + " coolingSpeed").increment(
					(long) (adaptationStrategy.returnCurrentCoolingSpeed(currentLayer, noOfLayers, noOfVertices, noOfEdges)*100));
			getContext().getCounter("Layer Counters", "Layer " + currentLayer + " tempFactor").increment(
					(long) (adaptationStrategy.returnCurrentInitialTempFactor(currentLayer, noOfLayers, noOfVertices, noOfEdges)*100));
			getContext().getCounter("Layer Counters", "Layer " + currentLayer + " accuracy").increment(
					(long) (adaptationStrategy.returnTargetAccuracyy(currentLayer, noOfLayers, noOfVertices, noOfEdges)*100000));
			getContext().getCounter(SolarMergerRoutine.COUNTER_GROUP, "Layer " + currentLayer + " vertices").increment(noOfVertices);
			getContext().getCounter(SolarMergerRoutine.COUNTER_GROUP, "Layer " + currentLayer + " edges").increment(noOfEdges);
		}
		//		if(angularMaximization){
		//			if(angularMaximizationIterationsMax > 0){
		//				if(getComputation().equals(CoordinatesBroadcast.class)){
		//					setAggregatedValue(LayoutRoutine.angleMaximizationClockwiseAggregator, new BooleanWritable(
		//							Math.random() > 0.5));
		//					setComputation(AngularResolutionMaximizer.class);
		//					return;
		//				}
		//				if(getComputation().equals(AngularResolutionMaximizer.class)){
		//					setComputation(AverageCoordinateUpdater.class);
		//					return;
		//				}
		//				if(angularMaximizationIterations < angularMaximizationIterationsMax){
		//					setComputation(CoordinatesBroadcast.class);
		//					angularMaximizationIterations++;
		//					return;
		//				}
		//			}
		//			angularMaximizationIterations = 0;
		//			angularMaximization = false;
		//
		//			layout = false;
		//
		//			if(currentLayer > 0){
		//				placing = true;
		//			}else{
		//				placing = false;
		//				reintegrating = true;
		//				return;
		//			}
		//		}
		if(currentLayer >= 0 && !reintegrating){
			int currentEdgeWeight = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerEdgeWeightsAggregator)).get(new IntWritable(currentLayer))).get();
			float optimalEdgeLength = (float)currentEdgeWeight;
			log.info("Suggested currentEdgeWeight " + currentEdgeWeight);
			//			if(noOfEdges > 0)
			//				optimalEdgeLength = currentEdgeWeight/(float)noOfEdges;
			//			else
			//				optimalEdgeLength = 1;
			optimalEdgeLength *= ((FloatWritable)getAggregatedValue(LayoutRoutine.k_agg)).get();

			log.info("Edge data computed: weight " + currentEdgeWeight + " " + optimalEdgeLength*((FloatWritable)getAggregatedValue(LayoutRoutine.k_agg)).get());
			setAggregatedValue(LayoutRoutine.walshawConstant_agg, 
					new FloatWritable(getConf().getFloat(LayoutRoutine.repulsiveForceModerationString,(float) (Math.pow(optimalEdgeLength, 2) * getConf().getFloat(LayoutRoutine.walshawModifierString, LayoutRoutine.walshawModifierDefault)))));

			if(layout){
				if(!layoutRoutine.compute(noOfVertices, optimalEdgeLength)){
					return;
				}else{
					layout = false;
					if(currentLayer > 0){
						placing = true;
					}else{
						placing = false;
						reintegrating = true;
					}
					//					if(currentLayer == 0){
					////						angularMaximization = true;
					////						setComputation(CoordinatesBroadcast.class);
					////						return;
					//						reintegrating = true;
					//					}else
					//					placing = true;

					//					setComputation(CoordinatesBroadcast.class);
					//					angularMaximization = true;
				}
			}
			if(placing)
				if(!placerRoutine.compute())
					return;
				else{
					placing = false;
					layout = true;
					log.info("deactivated placer");
					resetLayoutAggregators();

					//					layoutRoutine.compute(noOfVertices, optimalEdgeLength);

					int selectedK = adaptationStrategy.returnCurrentK(currentLayer, noOfLayers, 
							noOfVertices, 
							noOfEdges);
					if(currentLayer == 0)
						selectedK = (selectedK > 2 ? 2 : selectedK);
					setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(selectedK));
					setAggregatedValue(LayoutRoutine.coolingSpeedAggregator, new FloatWritable(
							adaptationStrategy.returnCurrentCoolingSpeed(currentLayer, noOfLayers, noOfVertices, noOfEdges)));
					setAggregatedValue(LayoutRoutine.initialTempFactorAggregator, new FloatWritable(
							adaptationStrategy.returnCurrentInitialTempFactor(currentLayer, noOfLayers, noOfVertices, noOfEdges)));
					setAggregatedValue(LayoutRoutine.currentAccuracyAggregator, new FloatWritable(
							adaptationStrategy.returnTargetAccuracyy(currentLayer, noOfLayers, noOfVertices, noOfEdges)));


					getContext().getCounter("Layer Counters", "Layer " + currentLayer + " k").increment(selectedK);
					getContext().getCounter("Layer Counters", "Layer " + currentLayer + " coolingSpeed").increment(
							(long) (adaptationStrategy.returnCurrentCoolingSpeed(currentLayer, noOfLayers, noOfVertices, noOfEdges)*100));
					getContext().getCounter("Layer Counters", "Layer " + currentLayer + " tempFactor").increment(
							(long) (adaptationStrategy.returnCurrentInitialTempFactor(currentLayer, noOfLayers, noOfVertices, noOfEdges)*100));
					getContext().getCounter("Layer Counters", "Layer " + currentLayer + " accuracy").increment(
							(long) (adaptationStrategy.returnTargetAccuracyy(currentLayer, noOfLayers, noOfVertices, noOfEdges)*100000));
					getContext().getCounter(SolarMergerRoutine.COUNTER_GROUP, "Layer " + currentLayer + " vertices").increment(noOfVertices);
					getContext().getCounter(SolarMergerRoutine.COUNTER_GROUP, "Layer " + currentLayer + " edges").increment(noOfEdges);

					return;
				}

		}
		if(reintegrating)
			if(reintegrationRoutine.compute()){
				terminate = true;
				haltComputation();
			}
	}

	/**
	 * 
	 */
	private void resetLayoutAggregators() {
		setAggregatedValue(LayoutRoutine.componentNumber, new IntWritable(0));
		setAggregatedValue(LayoutRoutine.componentNoOfNodes, new MapWritable());
		setAggregatedValue(LayoutRoutine.maxCoords, new MapWritable());
		setAggregatedValue(LayoutRoutine.minCoords, new MapWritable());
	}

}
