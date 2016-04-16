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
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleGraphExplorer;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleDrawingScaler;
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

		try {
			Class<? extends AdaptationStrategy> tClass = (Class<? extends AdaptationStrategy>) Class.forName(getConf().getStrings(adaptationStrategyString, SizeAndDensityDrivenAdaptationStrategy.class.toString())[0]);
			adaptationStrategy = tClass.getConstructor().newInstance();
		} catch (Exception e) {
			log.info("Caught exceptione");
			adaptationStrategy = new SizeAndDensityDrivenAdaptationStrategy();
		} 

		registerPersistentAggregator(LayoutRoutine.ttlMaxAggregator, IntMaxAggregator.class);

	}

	public void compute() {
		if(getSuperstep() == 0){
			merging = true;
		}
		if(terminate){
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

			setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(
					adaptationStrategy.returnCurrentK(currentLayer, noOfLayers, 
							noOfVertices, 
							noOfEdges)));

		}
		if(currentLayer >= 0 && !reintegrating){
			int currentEdgeWeight = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerEdgeWeightsAggregator)).get(new IntWritable(currentLayer))).get();
			float optimalEdgeLength;
			if(noOfEdges > 0)
				optimalEdgeLength = currentEdgeWeight/(float)noOfEdges;
			else
				optimalEdgeLength = 1;
			
			log.info("Edge data computed: weight " + currentEdgeWeight + " noOfEdges " + " " + noOfEdges + " optimalEdgeLength " + optimalEdgeLength);
			setAggregatedValue(LayoutRoutine.walshawConstant_agg, 
					new FloatWritable(getConf().getFloat(LayoutRoutine.repulsiveForceModerationString,(float) (Math.pow(optimalEdgeLength, 2) * getConf().getFloat(LayoutRoutine.walshawModifierString, LayoutRoutine.walshawModifierDefault)))));

			if(layout){
						
				if(!layoutRoutine.compute(noOfVertices, optimalEdgeLength)){
					return;
				}else{
					if(currentLayer > 0){
						layout = false;
						placing = true;
					}else
						reintegrating = true;
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
					
					layoutRoutine.compute(noOfVertices, optimalEdgeLength);
					setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(
							adaptationStrategy.returnCurrentK(currentLayer, noOfLayers, 
									noOfVertices, 
									noOfEdges)));
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
