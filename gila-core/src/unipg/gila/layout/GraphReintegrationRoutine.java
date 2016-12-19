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
import org.apache.hadoop.io.MapWritable;

import unipg.gila.common.datastructures.DoubleWritableArray;
import unipg.gila.layout.GraphReintegration.FairShareReintegrateOneEdges;
import unipg.gila.layout.GraphReintegration.PlainDummyComputation;
import unipg.gila.layout.LayoutRoutine.DrawingBoundariesExplorerWithComponentsNo;
import unipg.gila.layout.LayoutRoutine.LayoutCCs;

import com.google.common.collect.Lists;

/**
 * @author Alessio Arleo
 *
 */
public class GraphReintegrationRoutine {

  MasterCompute master;
  public static final String executeReintegrationString = "multi.layout.reintegrateOneDegrees";
  int readyToSleep;

  public void initialize(MasterCompute myMaster) {
    master = myMaster;
    readyToSleep = 0;
  }

  public boolean compute() {
    if (!master.getConf().getBoolean(executeReintegrationString, true)
            && readyToSleep == 0)
      readyToSleep = 2;
    switch (readyToSleep) {
    case 0: // FIRST STEP: ONE DEGREE VERTICES REINTEGRATION
      try {
        master.setComputation((Class<? extends Computation>) Class
                .forName(master.getConf().get(
                        LayoutRoutine.oneDegreeReintegratingClassOption,
                        FairShareReintegrateOneEdges.class.toString())));
      } catch (ClassNotFoundException e) {
        master.setComputation(FairShareReintegrateOneEdges.class);
      }
      readyToSleep++;
      return false;
    case 1: // A BLANK COMPUTATION TO PROPAGATE THE GRAPH MODIFICATIONS MADE IN
            // THE PREVIOUS SUPERSTEP
      master.setComputation(PlainDummyComputation.class);
      readyToSleep++;
      return false;
    case 2: // SECOND STEP: TO COMPUTE THE FINAL GRID LAYOUT OF THE CONNECTED
            // COMPONENTS, THEIR DRAWING
      master.setAggregatedValue(LayoutRoutine.maxCoords, new MapWritable()); // PROPORTIONS
                                                                             // ARE
                                                                             // SCANNED.
      master.setAggregatedValue(LayoutRoutine.minCoords, new MapWritable());
      master.setComputation(DrawingBoundariesExplorerWithComponentsNo.class);
      readyToSleep++;
      return false;
    case 3: // THIRD STEP: ONCE THE DATA NEEDED TO LAYOUT THE CONNECTED
            // COMPONENTS GRID ARE COMPUTED
      computeComponentGridLayout(); // THE LAYOUT IS COMPUTED.
      master.setComputation(LayoutCCs.class);
      readyToSleep++;
      return false;
    default:
      return true;
    }

  }

  /**
   * This method computes the connected components final grid layout.
   */
  protected void computeComponentGridLayout() {

    double componentPadding = master.getConf().getDouble(
            LayoutRoutine.componentPaddingConfString,
            LayoutRoutine.defaultPadding);
    double minRatioThreshold = master.getConf().getDouble(
            LayoutRoutine.minRationThresholdString,
            LayoutRoutine.defaultMinRatioThreshold);

    MapWritable offsets = new MapWritable();

    MapWritable maxCoordsMap = master
            .getAggregatedValue(LayoutRoutine.maxCoords);
    MapWritable minCoordsMap = master
            .getAggregatedValue(LayoutRoutine.minCoords);
    MapWritable componentsNo = master
            .getAggregatedValue(LayoutRoutine.componentNoOfNodes);

    // ##### SORTER -- THE MAP CONTAINING THE COMPONENTS' SIZES IS SORTED BY ITS
    // VALUES

    LinkedHashMap<IntWritable, IntWritable> sortedMap = (LinkedHashMap<IntWritable, IntWritable>) sortMapByValues(componentsNo);

    IntWritable[] componentSizeSorter = sortedMap.keySet().toArray(
            new IntWritable[0]);
    IntWritable[] componentSizeSorterValues = sortedMap.values().toArray(
            new IntWritable[0]);

    int coloumnNo = new Double(Math.ceil(Math.sqrt(componentsNo.size() - 1)))
            .intValue();

    Point2D.Double cursor = new Point2D.Double(0.0f, 0.0f);
    Point2D.Double tableOrigin = new Point2D.Double(0.0f, 0.0f);

    int maxID = componentSizeSorter[componentSizeSorter.length - 1].get();
    int maxNo = componentSizeSorterValues[componentSizeSorter.length - 1]
            .get();// ((LongWritable)componentsNo.get(new
                   // LongWritable(maxID))).get();

    double[] translationCorrection = ((DoubleWritableArray) minCoordsMap
            .get(new IntWritable(maxID))).get();
    offsets.put(new IntWritable(maxID), new DoubleWritableArray(new double[] {
            -translationCorrection[0], -translationCorrection[1], 1.0f,
            cursor.x, cursor.y }));

    double[] maxComponents = ((DoubleWritableArray) maxCoordsMap
            .get(new IntWritable(maxID))).get();
    // float componentPadding =
    // getConf().getFloat(FloodingMaster.componentPaddingConfString,
    // defaultPadding)*maxComponents[0];
    cursor.setLocation((maxComponents[0] - translationCorrection[0])
            + componentPadding, 0.0f); // THE BIGGEST COMPONENT IS PLACED IN THE
                                       // UPPER LEFT CORNER.
    tableOrigin.setLocation(cursor);

    double coloumnMaxY = 0.0f;
    int counter = 1;

    for (int j = componentSizeSorter.length - 2; j >= 0; j--) { // THE OTHER
                                                                // SMALLER
                                                                // COMPONENTS
                                                                // ARE ARRANGED
                                                                // IN A GRID.
      int currentComponent = componentSizeSorter[j].get();
      maxComponents = ((DoubleWritableArray) maxCoordsMap.get(new IntWritable(
              currentComponent))).get();
      double sizeRatio = (double) componentSizeSorterValues[j].get() / maxNo;
      translationCorrection = ((DoubleWritableArray) minCoordsMap
              .get(new IntWritable(currentComponent))).get();

      if (sizeRatio < minRatioThreshold)
        sizeRatio = minRatioThreshold;

      maxComponents[0] -= translationCorrection[0];
      maxComponents[1] -= translationCorrection[1];
      maxComponents[0] *= sizeRatio;
      maxComponents[1] *= sizeRatio;

      offsets.put(new IntWritable(currentComponent),
              new DoubleWritableArray(
                      new double[] { -translationCorrection[0],
                              -translationCorrection[1], sizeRatio, cursor.x,
                              cursor.y }));
      if (maxComponents[1] > coloumnMaxY)
        coloumnMaxY = maxComponents[1];
      if (counter % coloumnNo != 0) {
        cursor.setLocation(cursor.x + maxComponents[0] + componentPadding,
                cursor.y);
        counter++;
      } else {
        cursor.setLocation(tableOrigin.x, cursor.y + coloumnMaxY
                + componentPadding);
        coloumnMaxY = 0.0f;
        counter = 1;
      }
    }
    master.setAggregatedValue(LayoutRoutine.offsetsAggregator, offsets); // THE
                                                                         // VALUES
                                                                         // COMPUTED
                                                                         // TO
                                                                         // LAYOUT
                                                                         // THE
                                                                         // COMPONENTS
                                                                         // ARE
                                                                         // STORED
                                                                         // INTO
                                                                         // AN
                                                                         // AGGREGATOR.
  }

  /**
   * This method sorts a map by its values.
   * 
   * @param mapToSort
   * @return The sorted LinkedHashMap.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected static LinkedHashMap sortMapByValues(Map mapToSort) {
    List keys = Lists.newArrayList(mapToSort.keySet());
    List values = Lists.newArrayList(mapToSort.values());

    Collections.sort(keys);
    Collections.sort(values);

    LinkedHashMap sortedMap = new LinkedHashMap(mapToSort.size());

    Iterator vals = values.iterator();

    while (vals.hasNext()) {
      Object currentVal = vals.next();
      Iterator keysToIterate = keys.iterator();

      while (keysToIterate.hasNext()) {
        Object currentKey = keysToIterate.next();
        if (mapToSort.get(currentKey).equals(currentVal)) {
          sortedMap.put(currentKey, currentVal);
          keys.remove(currentKey);
          break;
        }
      }
    }

    return sortedMap;

  }

}
