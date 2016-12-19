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
package unipg.gila.multi;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.MergerToPlacerDummyComputation;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.layout.AdaptationStrategy;
import unipg.gila.multi.layout.LayoutAdaptationStrategy.SizeDrivenAdaptationStrategy;
import unipg.gila.multi.layout.MultiScaleLayout;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleDrawingScaler;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleGraphExplorer;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleGraphExplorerWithComponentsNo;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleLayoutCC;
import unipg.gila.multi.placers.SolarPlacerRoutine;
import unipg.gila.multi.spanningtree.SpanningTreeCreationRoutine;

/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleMaster extends DefaultMasterCompute {

  // LOGGER
  Logger log = Logger.getLogger(getClass());

  public static final String adaptationStrategyString =
    "multi.layout.adaptationStrategy";
  public static final String adaptationStrategyOptionsString =
      "multi.layout.adaptationStrategyOptions";  

  public static final String multiCounterString = "Global Counters";

  LayoutRoutine layoutRoutine;
  SolarMergerRoutine mergerRoutine;
  SpanningTreeCreationRoutine spanningTreeRoutine;
  SolarPlacerRoutine placerRoutine;
  GraphReintegrationRoutine reintegrationRoutine;
  AdaptationStrategy adaptationStrategy;

  boolean merging;
  boolean placing;
  boolean layout;
  boolean reintegrating;
  boolean preparePlacer;
  boolean spanningTreeSetup;
  // boolean angularMaximization;
  boolean forceMaximization;
  int angularMaximizationIterations;
  int angularMaximizationIterationsMax;
  boolean terminate;
  
  int workers;

  @SuppressWarnings("unchecked")
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    layoutRoutine = new LayoutRoutine();
    layoutRoutine.initialize(this, MultiScaleLayout.Seeder.class,
      MultiScaleLayout.Propagator.class, MultiScaleGraphExplorer.class,
      MultiScaleGraphExplorerWithComponentsNo.class,
      MultiScaleDrawingScaler.class, MultiScaleLayoutCC.class);

    mergerRoutine = new SolarMergerRoutine();
    mergerRoutine.initialize(this);

    placerRoutine = new SolarPlacerRoutine();
    placerRoutine.initialize(this);

    reintegrationRoutine = new GraphReintegrationRoutine();
    reintegrationRoutine.initialize(this);

    spanningTreeRoutine = new SpanningTreeCreationRoutine();
    spanningTreeRoutine.initialize(this);

    merging = false;
    layout = false;
    reintegrating = false;
    spanningTreeSetup = false;
    preparePlacer = false;
    placing = false;
    terminate = false;

    workers = GiraphConstants.SPLIT_MASTER_WORKER.get(getConf()) ? getConf().getMapTasks() - 1 : getConf().getMapTasks();
       
    try {
      Class< ? extends AdaptationStrategy> tClass =
        (Class< ? extends AdaptationStrategy>) Class.forName(getConf()
          .getStrings(adaptationStrategyString,
            SizeDrivenAdaptationStrategy.class.toString())[0]);
      adaptationStrategy = tClass.getConstructor(String.class).newInstance(getConf().getStrings(adaptationStrategyOptionsString, "")[0]);
    } catch (Exception e) {
      log.info("Caught exception, switching to default");
      adaptationStrategy = new SizeDrivenAdaptationStrategy(getConf().getStrings(adaptationStrategyOptionsString, "")[0]);
    }

  }

  public void compute() {
    if (getSuperstep() == 0) {
      merging = true;
    }

    int currentLayer =
      ((IntWritable) getAggregatedValue(SolarMergerRoutine.currentLayer))
        .get();

    if (merging) {
      if (!mergerRoutine.compute()) {
        return;
      }
      else {
        merging = false;
        spanningTreeSetup = true;
      }
    }

    if (spanningTreeSetup)
      if (!spanningTreeRoutine.compute()) {
        return;
      }
      else {
        preparePlacer = true;
        setComputation(MergerToPlacerDummyComputation.class);
        spanningTreeSetup = false;
        return;
      }

    int noOfVertices =
      ((IntWritable) ((MapWritable) getAggregatedValue(SolarMergerRoutine.layerVertexSizeAggregator))
        .get(new IntWritable(currentLayer))).get();
    int noOfLayers =
      ((IntWritable) getAggregatedValue(SolarMergerRoutine.layerNumberAggregator))
        .get();
    int noOfEdges =
      ((IntWritable) ((MapWritable) getAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator))
        .get(new IntWritable(currentLayer))).get();

    if (preparePlacer) {
      preparePlacer = false;
      layout = true;

      updateCountersAndAggregators(currentLayer, noOfLayers, noOfVertices,
        noOfEdges);
    }

    if (currentLayer >= 0 && !reintegrating) {

      if (layout) {
        int currentEdgeWeight =
          ((IntWritable) ((MapWritable) getAggregatedValue(SolarMergerRoutine.layerEdgeWeightsAggregator))
            .get(new IntWritable(currentLayer))).get();
        double optimalEdgeLength = (double) currentEdgeWeight;

        optimalEdgeLength *=
          ((DoubleWritable) getAggregatedValue(LayoutRoutine.k_agg)).get();

        setAggregatedValue(LayoutRoutine.walshawConstant_agg,
          new DoubleWritable(getConf().getDouble(
            LayoutRoutine.repulsiveForceModerationString,
            Math.pow(optimalEdgeLength, 2 * getConf().getDouble(
              LayoutRoutine.walshawModifierString,
              LayoutRoutine.walshawModifierDefault)))));

        if (!layoutRoutine.compute(noOfVertices, optimalEdgeLength)) {
          return;
        }
        else {
          layout = false;
          if (currentLayer > 0) {
            placing = true;
          }
          else {
            reintegrating = true;
          }

        }
      }
      if (placing)
        if (!placerRoutine.compute())
          return;
        else {
          placing = false;
          layout = true;
          resetLayoutAggregators();
          updateCountersAndAggregators(currentLayer, noOfLayers, noOfVertices,
            noOfEdges);
          return;
        }

    }
    if (reintegrating)
      if (reintegrationRoutine.compute()) {
        getContext().getCounter(multiCounterString, "Supersteps").increment(
          getSuperstep());
        getContext().getCounter("Messages Statistics",
          "Total Aggregated Messages").increment(
          getContext().getCounter(GiraphStats.GROUP_NAME,
            GiraphStats.AGGREGATE_SENT_MESSAGES_NAME).getValue());
        haltComputation();
      }
  }

  private void updateCountersAndAggregators(int currentLayer, int noOfLayers,
    int noOfVertices, int noOfEdges) {
    int selectedK = adaptationStrategy.returnCurrentK(currentLayer, noOfLayers,
        noOfVertices, noOfEdges, workers);
    double coolingSpeed =
      adaptationStrategy.returnCurrentCoolingSpeed(currentLayer, noOfLayers,
        noOfVertices, noOfEdges);
    double initialTemp =
      adaptationStrategy.returnCurrentInitialTempFactor(currentLayer,
        noOfLayers, noOfVertices, noOfEdges);
    double accuracy =
      adaptationStrategy.returnTargetAccuracyy(currentLayer, noOfLayers,
        noOfVertices, noOfEdges);
    
    setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(
      selectedK));
    setAggregatedValue(LayoutRoutine.coolingSpeedAggregator,
      new DoubleWritable(coolingSpeed));
    setAggregatedValue(LayoutRoutine.initialTempFactorAggregator,
      new DoubleWritable(initialTemp));
    setAggregatedValue(LayoutRoutine.currentAccuracyAggregator,
      new DoubleWritable(accuracy));

    getContext().getCounter("Layer Counters", "Layer " + currentLayer + " k")
      .increment(selectedK);
    getContext().getCounter("Layer Counters",
      "Layer " + currentLayer + " coolingSpeed").increment(
      (long) (coolingSpeed * 100));
    getContext().getCounter("Layer Counters",
      "Layer " + currentLayer + " tempFactor").increment(
      (long) (initialTemp * 100));
    getContext().getCounter("Layer Counters",
      "Layer " + currentLayer + " accuracy").increment(
      (long) (accuracy * 100000));
  }

  /**
   * 
   */
  private void resetLayoutAggregators() {
    setAggregatedValue(LayoutRoutine.max_K_agg, new DoubleWritable(
      Double.MIN_VALUE));
    setAggregatedValue(LayoutRoutine.componentNoOfNodes, new MapWritable());
    setAggregatedValue(LayoutRoutine.maxCoords, new MapWritable());
    setAggregatedValue(LayoutRoutine.minCoords, new MapWritable());
  }

}
