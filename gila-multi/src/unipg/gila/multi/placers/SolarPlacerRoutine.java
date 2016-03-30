/**
 * 
 */
package unipg.gila.multi.placers;

import org.apache.giraph.master.MasterCompute;

import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.CoordinatesBroadcast;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.InterLayerDataTransferComputation;

/**
 * @author Alessio Arleo
 *
 */
public class SolarPlacerRoutine {
	
	MasterCompute master;
	int counter;
	
	public void initialize(MasterCompute myMaster){
		master = myMaster;
		reset();
	}
	
	public boolean compute(){
		switch(counter){
		case 0 : master.setComputation(CoordinatesBroadcast.class); counter++; return false;
		case 1 : master.setComputation(InterLayerDataTransferComputation.class); counter++; return false;
		case 2 : counter++; return false;
		default: reset(); return true;
		}
	}
	
	/**
	 * 
	 */
	private void reset() {
		counter = 0;
	}
	
	

}
