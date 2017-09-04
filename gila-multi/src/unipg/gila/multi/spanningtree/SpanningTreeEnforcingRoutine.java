/**
 * 
 */
package unipg.gila.multi.spanningtree;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

import unipg.gila.multi.MultiScaleMaster;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.CoordinatesBroadcast;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.InterLayerDataTransferComputation;
import unipg.gila.multi.placers.SolarPlacer;
import unipg.gila.multi.spanningtree.SpanningTreeCreationRoutine.ExpandSpanningTree;
import unipg.gila.multi.spanningtree.SpanningTreeCreationRoutine.SpanningTreeConsistencyEnforcerForMoons;
import unipg.gila.multi.spanningtree.SpanningTreeCreationRoutine.SpanningTreeConsistencyEnforcerForPlanets;

/**
 * @author spark
 *
 */
public class SpanningTreeEnforcingRoutine {

	//PLACER OPTIONS
	public static final String logEnforcerRoutine = "spenforcer.showLog";

	//INSTANCE VARIABLES
	MasterCompute master;
	int counter;

	//GLOBAL STATIC VARIABLES
	public static boolean logPlacer;

	public void initialize(MasterCompute myMaster){
		master = myMaster;
		logPlacer = master.getConf().getBoolean(logEnforcerRoutine, false);
		reset();
	}

	public boolean compute(){
		switch(counter){
		case 0 : master.setComputation(ExpandSpanningTree.class); counter++; return false;
		case 1 : master.setComputation(SpanningTreeConsistencyEnforcerForMoons.class); counter++; return false;
		case 2 : master.setComputation(SpanningTreeConsistencyEnforcerForPlanets.class); counter++; return false;
		default : reset(); return true;
		}
	}

	/**
	 * 
	 */
	private void reset() {
		counter = 0;
	}

}
