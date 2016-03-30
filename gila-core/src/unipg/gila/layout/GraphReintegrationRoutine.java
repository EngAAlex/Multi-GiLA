/**
 * 
 */
package unipg.gila.layout;

import java.awt.geom.Point2D;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.Computation;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

import com.google.common.collect.Lists;

import unipg.gila.common.datastructures.FloatWritableArray;
import unipg.gila.layout.GraphReintegration.FairShareReintegrateOneEdges;
import unipg.gila.layout.GraphReintegration.PlainDummyComputation;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorer;
import unipg.gila.layout.LayoutRoutine.LayoutCCs;

/**
 * @author Alessio Arleo
 *
 */
public class GraphReintegrationRoutine {

	MasterCompute master;
	int readyToSleep;
	
	public void initialize(MasterCompute myMaster){
		master = myMaster;
		readyToSleep = 0;
	}
	
	public boolean compute(){
		switch(readyToSleep){
		case 0: 	//FIRST STEP: ONE DEGREE VERTICES REINTEGRATION		
			try {
			master.setComputation((Class<? extends Computation>)Class.forName(master.getConf().get(LayoutRoutine.oneDegreeReintegratingClassOption, FairShareReintegrateOneEdges.class.toString())));
		} catch (ClassNotFoundException e) {
			master.setComputation(FairShareReintegrateOneEdges.class);
		}	
		readyToSleep++;								
		return false;
		case 1: //A BLANK COMPUTATION TO PROPAGATE THE GRAPH MODIFICATIONS MADE IN THE PREVIOUS SUPERSTEP
			master.setComputation(PlainDummyComputation.class);
			readyToSleep++;
			return false;
		case 2: //SECOND STEP: TO COMPUTE THE FINAL GRID LAYOUT OF THE CONNECTED COMPONENTS, THEIR DRAWING
			master.setAggregatedValue(LayoutRoutine.maxCoords, new MapWritable()); //PROPORTIONS ARE SCANNED.
			master.setAggregatedValue(LayoutRoutine.minCoords, new MapWritable());
			master.setComputation(DrawingBoundariesExplorer.class);
			readyToSleep++;
			return false;
		case 3: //THIRD STEP: ONCE THE DATA NEEDED TO LAYOUT THE CONNECTED COMPONENTS GRID ARE COMPUTED
			computeComponentGridLayout(); //THE LAYOUT IS COMPUTED.
			master.setComputation(LayoutCCs.class);			
			readyToSleep++;
			return false;
		default: return true;
		}
		
	}
	
	/**
	 * This method computes the connected components final grid layout.
	 */
	protected void computeComponentGridLayout() {
		
		float componentPadding = master.getConf().getFloat(LayoutRoutine.componentPaddingConfString, LayoutRoutine.defaultPadding);
		float minRatioThreshold = master.getConf().getFloat(LayoutRoutine.minRationThresholdString, LayoutRoutine.defaultMinRatioThreshold );
		
		MapWritable offsets = new MapWritable();
				
		MapWritable maxCoordsMap = master.getAggregatedValue(LayoutRoutine.maxCoords);
		MapWritable minCoordsMap = master.getAggregatedValue(LayoutRoutine.minCoords);
		MapWritable componentsNo = master.getAggregatedValue(LayoutRoutine.componentNoOfNodes);
		
		//##### SORTER -- THE MAP CONTAINING THE COMPONENTS' SIZES IS SORTED BY ITS VALUES
		
		LinkedHashMap<LongWritable, IntWritable> sortedMap = (LinkedHashMap<LongWritable, IntWritable>)sortMapByValues(componentsNo);
	
		LongWritable[] componentSizeSorter = sortedMap.keySet().toArray(new LongWritable[0]);
		IntWritable[] componentSizeSorterValues = sortedMap.values().toArray(new IntWritable[0]);
				
		int coloumnNo = new Double(Math.ceil(Math.sqrt(componentsNo.size() - 1))).intValue();
		
		Point2D.Float cursor = new Point2D.Float(0.0f, 0.0f);
		Point2D.Float tableOrigin = new Point2D.Float(0.0f, 0.0f);
		
		Long maxID = componentSizeSorter[componentSizeSorter.length-1].get();
		int maxNo = componentSizeSorterValues[componentSizeSorter.length-1].get();// ((LongWritable)componentsNo.get(new LongWritable(maxID))).get();
		
		float[] translationCorrection = ((FloatWritableArray)minCoordsMap.get(new LongWritable(maxID))).get();
		offsets.put(new LongWritable(maxID), new FloatWritableArray(new float[]{-translationCorrection[0], -translationCorrection[1], 1.0f, cursor.x, cursor.y}));
		
		float[] maxComponents = ((FloatWritableArray)maxCoordsMap.get(new LongWritable(maxID))).get();
//		float componentPadding = getConf().getFloat(FloodingMaster.componentPaddingConfString, defaultPadding)*maxComponents[0];
		cursor.setLocation((maxComponents[0] - translationCorrection[0]) + componentPadding, 0.0f); //THE BIGGEST COMPONENT IS PLACED IN THE UPPER LEFT CORNER.
		tableOrigin.setLocation(cursor);
		
		float coloumnMaxY = 0.0f;
		int counter = 1;
		
		for(int j=componentSizeSorter.length-2; j>=0; j--){ //THE OTHER SMALLER COMPONENTS ARE ARRANGED IN A GRID.
			long currentComponent = componentSizeSorter[j].get();
			maxComponents = ((FloatWritableArray)maxCoordsMap.get(new LongWritable(currentComponent))).get();
			float sizeRatio = (float)componentSizeSorterValues[j].get()/maxNo;
			translationCorrection = ((FloatWritableArray)minCoordsMap.get(new LongWritable(currentComponent))).get();
						
			if(sizeRatio < minRatioThreshold)	
				sizeRatio = minRatioThreshold;
			
			maxComponents[0] -= translationCorrection[0];
			maxComponents[1] -= translationCorrection[1];
			maxComponents[0] *= sizeRatio;
			maxComponents[1] *= sizeRatio;
			
			offsets.put(new LongWritable(currentComponent), new FloatWritableArray(new float[]{-translationCorrection[0], -translationCorrection[1], sizeRatio,  cursor.x, cursor.y}));
			if(maxComponents[1] > coloumnMaxY)
				coloumnMaxY = maxComponents[1];
			if(counter % coloumnNo != 0){
				cursor.setLocation(cursor.x + maxComponents[0] + componentPadding, cursor.y);
				counter++;
			}else{
				cursor.setLocation(tableOrigin.x, cursor.y + coloumnMaxY + componentPadding);
				coloumnMaxY = 0.0f;
				counter = 1;
			}
		}
		master.setAggregatedValue(LayoutRoutine.offsetsAggregator, offsets); //THE VALUES COMPUTED TO LAYOUT THE COMPONENTS ARE STORED INTO AN AGGREGATOR.
	}
	
	/**
	 * This method sorts a map by its values.
	 * 
	 * @param mapToSort
	 * @return The sorted LinkedHashMap.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static LinkedHashMap sortMapByValues(Map mapToSort){
		List keys = Lists.newArrayList(mapToSort.keySet());
		List values = Lists.newArrayList(mapToSort.values());
				
		Collections.sort(keys);
		Collections.sort(values);	
		
		LinkedHashMap sortedMap = new LinkedHashMap(mapToSort.size());
		
		Iterator vals = values.iterator();
		
		while(vals.hasNext()){
			Object currentVal = vals.next();
			Iterator keysToIterate = keys.iterator();
			
			while(keysToIterate.hasNext()){
				Object currentKey = keysToIterate.next();
				if(mapToSort.get(currentKey).equals(currentVal)){
					sortedMap.put(currentKey, currentVal);
					keys.remove(currentKey);
					break;
				}	
			}
		}
		
		return sortedMap;
		
	}
	
}
