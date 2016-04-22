/**
 * 
 */
package unipg.gila.multi.placers;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;

import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.CoordinatesBroadcast;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.InterLayerDataTransferComputation;

/**
 * @author Alessio Arleo
 *
 */
public class SolarPlacerRoutine {
	
	//PLACER OPTIONS
	public static final String logPlacerString = "placer.showLog";
	
	//INSTANCE VARIABLES
	MasterCompute master;
	int counter;
	
	//GLOBAL STATIC VARIABLES
	public static boolean logPlacer;
	
	public void initialize(MasterCompute myMaster){
		master = myMaster;
		logPlacer = master.getConf().getBoolean(logPlacerString, false);
		reset();
	}
	
	public boolean compute(){
		int currentLayer = ((IntWritable)master.getAggregatedValue(SolarMergerRoutine.currentLayer)).get();
//		counter = ((currentLayer == 0 && counter == 0) ? 2 : counter);
		switch(counter){
		case 0 : master.setComputation(CoordinatesBroadcast.class); counter++; return false;
		case 1 : master.setComputation(InterLayerDataTransferComputation.class); counter++; return false;
		case 2 : master.setComputation(SolarPlacer.class);
			if(currentLayer > 0)
				master.setAggregatedValue(SolarMergerRoutine.currentLayer, new IntWritable(currentLayer - 1));
			counter++; return false;
		case 3 : master.setComputation(PlacerCoordinateDelivery.class); counter++; return false;
		default : reset(); return true;
//		default: reset(); if(currentLayer == 0) return true; else return false;
		}
	}
	
	/**
	 * 
	 */
	private void reset() {
		counter = 0;
	}
	
	

}
