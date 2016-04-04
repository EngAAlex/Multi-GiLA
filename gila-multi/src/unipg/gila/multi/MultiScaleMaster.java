/**
 * 
 */
package unipg.gila.multi;

import java.lang.reflect.InvocationTargetException;

import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.layout.AdaptationStrategy;
import unipg.gila.multi.layout.LayoutAdaptationStrategy.SizeAndDensityDrivenAdaptationStrategy;
import unipg.gila.multi.placers.SolarPlacerRoutine;

/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleMaster extends DefaultMasterCompute {
	
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
		if(merging)
			if(!mergerRoutine.compute())
				return;
			else{
				merging = false;
				placing = true;
			}
		int currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get(); 
		while(currentLayer >= 0 && !reintegrating){
			if(placing)
				if(!placerRoutine.compute())
					return;
				else{
					placing = false;
					layout = true;
					int noOfVertices = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerVertexSizeAggregator)).get(new IntWritable(currentLayer))).get();
					int noOfEdges = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator)).get(new IntWritable(currentLayer))).get();
					setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(
							adaptationStrategy.returnCurrentK(currentLayer, ((IntWritable)getAggregatedValue(SolarMergerRoutine.layerNumberAggregator)).get(), 
									noOfVertices, 
									noOfEdges)));
				}

			if(layout){
				if(!layoutRoutine.compute()){
					return;
				}else{
					if(currentLayer > 0){
						setAggregatedValue(SolarMergerRoutine.currentLayer, new IntWritable(currentLayer - 1));
						layout = false;
						placing = true;
						return;
					}else{
						reintegrating = true;
						reintegrationRoutine.compute();
					}
				}
			}
		}
		if(reintegrating)
			if(reintegrationRoutine.compute())
				haltComputation();

	}

}
