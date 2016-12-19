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
package unipg.gila.layout.single;

import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.layout.single.SingleScaleLayout.SingleDrawingExplorer;
import unipg.gila.layout.single.SingleScaleLayout.SingleDrawingExplorerWithComponentsNo;
import unipg.gila.layout.single.SingleScaleLayout.SingleDrawingScaler;
import unipg.gila.layout.single.SingleScaleLayout.SingleLayoutCCs;
import unipg.gila.layout.single.SingleScaleLayout.SinglePropagator;
import unipg.gila.layout.single.SingleScaleLayout.SingleSeeder;

/**
 * @author Alessio Arleo
 *
 */
public class SingleScaleMaster extends DefaultMasterCompute {

  LayoutRoutine layoutRoutine;
  GraphReintegrationRoutine reintegrationRoutine;
  double k = 0.0f;

  boolean layoutCompleted;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.giraph.master.DefaultMasterCompute#initialize()
   */
  @Override
  public void initialize() throws InstantiationException,
          IllegalAccessException {
    layoutRoutine = new LayoutRoutine();
    layoutRoutine.initialize(this, SingleSeeder.class, SinglePropagator.class,
            SingleDrawingExplorer.class,
            SingleDrawingExplorerWithComponentsNo.class,
            SingleDrawingScaler.class, SingleLayoutCCs.class);

    reintegrationRoutine = new GraphReintegrationRoutine();
    reintegrationRoutine.initialize(this);

    setAggregatedValue(
            LayoutRoutine.ttlMaxAggregator,
            new IntWritable(getConf().getInt(LayoutRoutine.ttlMaxString,
                    LayoutRoutine.ttlMaxDefault)));
    setAggregatedValue(
            LayoutRoutine.currentAccuracyAggregator,
            new DoubleWritable(getConf().getDouble(LayoutRoutine.accuracyString,
                    LayoutRoutine.accuracyDefault)));
    setAggregatedValue(
            LayoutRoutine.coolingSpeedAggregator,
            new DoubleWritable(getConf().getDouble(LayoutRoutine.coolingSpeed,
                    LayoutRoutine.defaultCoolingSpeed)));
    setAggregatedValue(
            LayoutRoutine.initialTempFactorAggregator,
            new DoubleWritable(getConf().getDouble(
                    LayoutRoutine.initialTempFactorString,
                    LayoutRoutine.defaultInitialTempFactor)));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.giraph.master.DefaultMasterCompute#compute()
   */
  @Override
  public void compute() {
    if (k == 0.0)
      k = ((DoubleWritable) getAggregatedValue(LayoutRoutine.k_agg)).get();

    if (!layoutCompleted) {
      if (layoutRoutine.compute(getTotalNumVertices(), k)) {
        layoutCompleted = true;
        reintegrationRoutine.compute();
      }
    } else if (reintegrationRoutine.compute())
      haltComputation();
  }

}
