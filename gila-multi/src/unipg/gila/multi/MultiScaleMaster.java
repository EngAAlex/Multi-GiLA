/**
 * 
 */
package unipg.gila.multi;

import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.placers.SolarPlacerRoutine;

/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleMaster extends DefaultMasterCompute {

	LayoutRoutine layoutRoutine;
	SolarMergerRoutine mergerRoutine;
	SolarPlacerRoutine placerRoutine;
	GraphReintegrationRoutine reintegrationRoutine;

	boolean merging;
	boolean placing;
	boolean layout;
	boolean reintegrating;
	
	public void initialize() throws InstantiationException ,IllegalAccessException {
		layoutRoutine = new LayoutRoutine();
		layoutRoutine.initialize(this, MultiScaleLayout.Seeder.class, MultiScaleLayout.Propagator.class);

		mergerRoutine = new SolarMergerRoutine();
		mergerRoutine.initialize(this);

		placerRoutine = new SolarPlacerRoutine();
		placerRoutine.initialize(this);
		
		reintegrationRoutine = new GraphReintegrationRoutine();
		reintegrationRoutine.initialize(this);
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
				}

			if(layout)
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
		if(reintegrating)
			if(reintegrationRoutine.compute())
				haltComputation();

	}

}
