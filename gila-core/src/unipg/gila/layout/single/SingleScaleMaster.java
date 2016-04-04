/**
 * 
 */
package unipg.gila.layout.single;

import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;

/**
 * @author Alessio Arleo
 *
 */
public class SingleScaleMaster extends DefaultMasterCompute{
	
	LayoutRoutine layoutRoutine;
	GraphReintegrationRoutine reintegrationRoutine;
	
	boolean layoutCompleted;
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.master.DefaultMasterCompute#initialize()
	 */
	@Override
	public void initialize() throws InstantiationException,
			IllegalAccessException {
		layoutRoutine = new LayoutRoutine();
		layoutRoutine.initialize(this, SingleScaleLayout.Seeder.class, SingleScaleLayout.Propagator.class);
		
		reintegrationRoutine = new GraphReintegrationRoutine();
		reintegrationRoutine.initialize(this);
		
		registerPersistentAggregator(LayoutRoutine.ttlMaxAggregator, IntMaxAggregator.class);
		
		setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(getConf().getInt(LayoutRoutine.ttlMaxString, LayoutRoutine.ttlMaxDefault)));
	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.master.DefaultMasterCompute#compute()
	 */
	@Override
	public void compute() {
		if(!layoutCompleted){
			if(layoutRoutine.compute())
				layoutCompleted = true;
		}else
			if(reintegrationRoutine.compute())
				haltComputation();
	}	

}
