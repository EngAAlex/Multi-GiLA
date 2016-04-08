/**
 * 
 */
package unipg.gila.multi;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.MergerToPlacerDummyComputation;
import unipg.gila.multi.layout.AdaptationStrategy;
import unipg.gila.multi.layout.MultiScaleLayout;
import unipg.gila.multi.layout.LayoutAdaptationStrategy.SizeAndDensityDrivenAdaptationStrategy;
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

	@SuppressWarnings("unchecked")
	public void initialize() throws InstantiationException ,IllegalAccessException {
		layoutRoutine = new LayoutRoutine();
		layoutRoutine.initialize(this, MultiScaleLayout.Seeder.class, MultiScaleLayout.Propagator.class);

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

		try {
			Class<? extends AdaptationStrategy> tClass = (Class<? extends AdaptationStrategy>) Class.forName(getConf().getStrings(adaptationStrategyString, SizeAndDensityDrivenAdaptationStrategy.class.toString())[0]);
			adaptationStrategy = tClass.getConstructor().newInstance();
		} catch (Exception e) {
			adaptationStrategy = new SizeAndDensityDrivenAdaptationStrategy();
		} 

		registerPersistentAggregator(LayoutRoutine.ttlMaxAggregator, IntMaxAggregator.class);

	}

	public void compute() {
		if(getSuperstep() == 0){
			merging = true;
		}
		int currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get(); 

		if(merging)
			if(!mergerRoutine.compute()){
				return;
			}
			else{
				merging = false;
				placing = true;
				preparePlacer = true;
				setComputation(MergerToPlacerDummyComputation.class);
				return;
			}
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

			int noOfVertices = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerVertexSizeAggregator)).get(new IntWritable(currentLayer))).get();

			int noOfEdges = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator)).get(new IntWritable(currentLayer))).get();
			setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(
					adaptationStrategy.returnCurrentK(currentLayer, ((IntWritable)getAggregatedValue(SolarMergerRoutine.layerNumberAggregator)).get(), 
							noOfVertices, 
							noOfEdges)));

		}
		if(currentLayer >= 0 && !reintegrating){
			int noOfVertices = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerVertexSizeAggregator)).get(new IntWritable(currentLayer))).get();
			if(layout){
				if(!layoutRoutine.compute(noOfVertices)){
					return;
				}else{
					if(currentLayer > 0){
						//						setAggregatedValue(SolarMergerRoutine.currentLayer, new IntWritable(currentLayer - 1));
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
					int noOfEdges = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator)).get(new IntWritable(currentLayer))).get();
					setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(
							adaptationStrategy.returnCurrentK(currentLayer, ((IntWritable)getAggregatedValue(SolarMergerRoutine.layerNumberAggregator)).get(), 
									noOfVertices, 
									noOfEdges)));
					return;
				}

		}
		if(reintegrating)
			if(reintegrationRoutine.compute())
				haltComputation();

	}

}
