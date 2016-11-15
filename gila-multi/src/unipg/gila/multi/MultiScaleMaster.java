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

import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.MergerToPlacerDummyComputation;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.layout.AdaptationStrategy;
import unipg.gila.multi.layout.LayoutAdaptationStrategy.SizeAndDensityDrivenAdaptationStrategy;
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

  //LOGGER
  Logger log = Logger.getLogger(getClass());

  public static final String adaptationStrategyString = "multi.layout.adaptationStrategy";

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
  //	boolean angularMaximization;
  boolean forceMaximization;
  int angularMaximizationIterations;
  int angularMaximizationIterationsMax;
  boolean terminate;

  @SuppressWarnings("unchecked")
  public void initialize() throws InstantiationException ,IllegalAccessException {
    layoutRoutine = new LayoutRoutine();
    layoutRoutine.initialize(this, MultiScaleLayout.Seeder.class, MultiScaleLayout.Propagator.class,
      MultiScaleGraphExplorer.class, MultiScaleGraphExplorerWithComponentsNo.class,
      MultiScaleDrawingScaler.class,
      MultiScaleLayoutCC.class);

    mergerRoutine = new SolarMergerRoutine();
    mergerRoutine.initialize(this);

    placerRoutine = new SolarPlacerRoutine();
    placerRoutine.initialize(this);

    reintegrationRoutine = new GraphReintegrationRoutine();
    reintegrationRoutine.initialize(this);

    spanningTreeRoutine = new SpanningTreeCreationRoutine();
    spanningTreeRoutine.initialize(this);
    
    merging=false;
    layout=false;
    reintegrating=false;
    spanningTreeSetup = false;
    preparePlacer = false;
    placing=false;
    terminate = false;

    try {
      Class<? extends AdaptationStrategy> tClass = (Class<? extends AdaptationStrategy>) Class.forName(getConf().getStrings(adaptationStrategyString, SizeAndDensityDrivenAdaptationStrategy.class.toString())[0]);
      adaptationStrategy = tClass.getConstructor().newInstance();
    } catch (Exception e) {
      log.info("Caught exceptione, swithcing to default");
      adaptationStrategy = new SizeAndDensityDrivenAdaptationStrategy();
    } 

  }

  public void compute() {
    if(getSuperstep() == 0){
      merging = true;
    }

    if(terminate){
      getContext().getCounter(multiCounterString, "Supersteps").increment(getSuperstep());
      haltComputation();
      return;
    }
    int currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get(); 

    if(merging){
      if(!mergerRoutine.compute()){
        return;
      }else{
        merging = false;
        spanningTreeSetup = true;
      }
    }
    
    if(spanningTreeSetup)
      if(!spanningTreeRoutine.compute()){
        return;
      }else{
        preparePlacer = true;
        setComputation(MergerToPlacerDummyComputation.class);
        spanningTreeSetup = false;
        return;
      }

    int noOfVertices = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerVertexSizeAggregator)).get(new IntWritable(currentLayer))).get();
    int noOfLayers = ((IntWritable)getAggregatedValue(SolarMergerRoutine.layerNumberAggregator)).get();
    int noOfEdges = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator)).get(new IntWritable(currentLayer))).get();

    if(preparePlacer){
      preparePlacer = false;
      layout = true;

      int selectedK = adaptationStrategy.returnCurrentK(currentLayer, noOfLayers, 
        noOfVertices, 
        noOfEdges);
      if(currentLayer == 0)
        selectedK = (selectedK > 2 ? 2 : selectedK);
      updateCountersAndAggregators(selectedK, currentLayer, noOfLayers, noOfVertices, noOfEdges);
    }

    if(currentLayer >= 0 && !reintegrating){

      if(layout){
        int currentEdgeWeight = ((IntWritable)((MapWritable)getAggregatedValue(SolarMergerRoutine.layerEdgeWeightsAggregator)).get(new IntWritable(currentLayer))).get();
        float optimalEdgeLength = (float)currentEdgeWeight;

        optimalEdgeLength *= ((FloatWritable)getAggregatedValue(LayoutRoutine.k_agg)).get();

        setAggregatedValue(LayoutRoutine.walshawConstant_agg, 
          new FloatWritable(getConf().getFloat(LayoutRoutine.repulsiveForceModerationString,(float) (Math.pow(optimalEdgeLength, 2) * getConf().getFloat(LayoutRoutine.walshawModifierString, LayoutRoutine.walshawModifierDefault)))));

        if(!layoutRoutine.compute(noOfVertices, optimalEdgeLength)){
          return;
        }else{
          layout = false;
          if(currentLayer > 0){
            placing = true;
          }else{
            reintegrating = true;
          }

        }
      }
      if(placing)
        if(!placerRoutine.compute())
          return;
        else{
          placing = false;
          layout = true;
          resetLayoutAggregators();
          int selectedK = adaptationStrategy.returnCurrentK(currentLayer, noOfLayers, 
            noOfVertices, 
            noOfEdges);
          if(currentLayer == 0)
            selectedK = (selectedK > 2 ? 2 : selectedK);
          updateCountersAndAggregators(selectedK, currentLayer, noOfLayers, noOfVertices, noOfEdges);

          return;
        }

    }
    if(reintegrating)
      if(reintegrationRoutine.compute()){
        terminate = true;
        haltComputation();
      }
  }

  private void updateCountersAndAggregators(int selectedK, int currentLayer, int noOfLayers, int noOfVertices, int noOfEdges){
    setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(selectedK));

    setAggregatedValue(LayoutRoutine.coolingSpeedAggregator, new FloatWritable(
      adaptationStrategy.returnCurrentCoolingSpeed(currentLayer, noOfLayers, noOfVertices, noOfEdges)));
    setAggregatedValue(LayoutRoutine.initialTempFactorAggregator, new FloatWritable(
      adaptationStrategy.returnCurrentInitialTempFactor(currentLayer, noOfLayers, noOfVertices, noOfEdges)));
    setAggregatedValue(LayoutRoutine.currentAccuracyAggregator, new FloatWritable(
      adaptationStrategy.returnTargetAccuracyy(currentLayer, noOfLayers, noOfVertices, noOfEdges)));

    getContext().getCounter("Layer Counters", "Layer " + currentLayer + " k").increment(selectedK);
    getContext().getCounter("Layer Counters", "Layer " + currentLayer + " coolingSpeed").increment(
      (long) (adaptationStrategy.returnCurrentCoolingSpeed(currentLayer, noOfLayers, noOfVertices, noOfEdges)*100));
    getContext().getCounter("Layer Counters", "Layer " + currentLayer + " tempFactor").increment(
      (long) (adaptationStrategy.returnCurrentInitialTempFactor(currentLayer, noOfLayers, noOfVertices, noOfEdges)*100));
    getContext().getCounter("Layer Counters", "Layer " + currentLayer + " accuracy").increment(
      (long) (adaptationStrategy.returnTargetAccuracyy(currentLayer, noOfLayers, noOfVertices, noOfEdges)*100000));
  }

  /**
   * 
   */
  private void resetLayoutAggregators() {
    setAggregatedValue(LayoutRoutine.max_K_agg, new FloatWritable(Float.MIN_VALUE));
    setAggregatedValue(LayoutRoutine.componentNoOfNodes, new MapWritable());
    setAggregatedValue(LayoutRoutine.maxCoords, new MapWritable());
    setAggregatedValue(LayoutRoutine.minCoords, new MapWritable());
  }

}
