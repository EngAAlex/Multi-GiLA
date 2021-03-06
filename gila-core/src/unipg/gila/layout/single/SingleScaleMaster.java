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

import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.utils.Toolbox;

/**
 * @author Alessio Arleo
 *
 */
public class SingleScaleMaster extends DefaultMasterCompute{
	
	LayoutRoutine layoutRoutine;
	GraphReintegrationRoutine reintegrationRoutine;
	float k;
	
	boolean layoutCompleted;
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.master.DefaultMasterCompute#initialize()
	 */
	@Override
	public void initialize() throws InstantiationException,
			IllegalAccessException {
		layoutRoutine = new LayoutRoutine();
		layoutRoutine.initialize(this, SingleScaleLayout.SingleSeeder.class, SingleScaleLayout.SinglePropagator.class,
								SingleScaleLayout.SingleDrawingExplorer.class, SingleScaleLayout.SingleDrawingExplorerWithComponentsNo.class, 
								SingleScaleLayout.SingleDrawingScaler.class, SingleScaleLayout.SingleLayoutCCs.class);
		
		reintegrationRoutine = new GraphReintegrationRoutine();
		reintegrationRoutine.initialize(this);
		
	
		float nl = getConf().getFloat(LayoutRoutine.node_length , LayoutRoutine.defaultNodeValue);
		float nw = getConf().getFloat(LayoutRoutine.node_width , LayoutRoutine.defaultNodeValue);
		float ns = getConf().getFloat(LayoutRoutine.node_separation, LayoutRoutine.defaultNodeValue);
		float k = new Double(ns + Toolbox.computeModule(new float[]{nl, nw})).floatValue();
//		setAggregatedValue(LayoutRoutine.k_agg, new FloatWritable(k));
	
		setAggregatedValue(LayoutRoutine.walshawConstant_agg, 
				new FloatWritable(getConf().getFloat(LayoutRoutine.repulsiveForceModerationString,(float) (Math.pow(k, 2) * getConf().getFloat(LayoutRoutine.walshawModifierString, LayoutRoutine.walshawModifierDefault)))));

	}
	
	/* (non-Javadoc)
	 * @see org.apache.giraph.master.DefaultMasterCompute#compute()
	 */
	@Override
	public void compute() {
		if(!layoutCompleted){
			if(layoutRoutine.compute(getTotalNumVertices(), k))
				layoutCompleted = true;
		}else
			if(reintegrationRoutine.compute())
				haltComputation();
	}	

}
