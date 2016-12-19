/*******************************************************************************
 * Copyright 2016 Alessio Arleo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
/**
 * 
 */
package unipg.gila.multi.placers;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;

import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.CoordinatesBroadcast;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.InterLayerDataTransferComputation;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.spanningtree.SpanningTreeCreationRoutine.SpanningTreeConsistencyEnforcerForMoons;
import unipg.gila.multi.spanningtree.SpanningTreeCreationRoutine.SpanningTreeConsistencyEnforcerForPlanets;

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
    case 4 : counter++; return false;
    case 5 : master.setComputation(SpanningTreeConsistencyEnforcerForMoons.class); counter++; return false;
    case 6 : master.setComputation(SpanningTreeConsistencyEnforcerForPlanets.class); counter++; return false;
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
