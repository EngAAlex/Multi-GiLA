/*******************************************************************************
s * Copyright 2016 Alessio Arleo
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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.layout.GraphReintegrationRoutine;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.MergerToPlacerDummyComputation;
import unipg.gila.multi.coarseners.SolarMerger;
import unipg.gila.multi.coarseners.SolarMergerRoutine;
import unipg.gila.multi.layout.AdaptationStrategy;
import unipg.gila.multi.layout.LayoutAdaptationStrategy.SizeAndDiameterDrivenAdaptationStrategy;
import unipg.gila.multi.layout.LayoutAdaptationStrategy.SizeDiameterEnvironmentAwareDrivenAdaptationStrategy;
import unipg.gila.multi.layout.MultiScaleLayout;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleDrawingScaler;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleGraphExplorer;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleGraphExplorerWithComponentsNo;
import unipg.gila.multi.layout.MultiScaleLayout.MultiScaleLayoutCC;
import unipg.gila.multi.placers.SolarPlacerRoutine;
import unipg.gila.multi.spanningtree.SpanningTreeCreationRoutine;
import unipg.gila.multi.spanningtree.SpanningTreeEnforcingRoutine;

/**
 * @author Alessio Arleo
 *
 */
public class MultiScaleMaster extends DefaultMasterCompute {

	// LOGGER
	Logger log = Logger.getLogger(getClass());

	public static final String mergeOnlyString = "multi.mergeOnly";
	public static final String skipMergingString = "multi.skipMerging";
	public static final String adaptationStrategyString =
			"multi.layout.adaptationStrategy";
	public static final String adaptationStrategyOptionsString =
			"multi.layout.adaptationStrategyOptions";  

	public static final String multiCounterString = "Global Counters";

	public static final String thresholdSurpassedAggregator = "THR_AGG";

	LayoutRoutine layoutRoutine;
	SolarMergerRoutine mergerRoutine;
	SpanningTreeCreationRoutine spanningTreeRoutine;
	SolarPlacerRoutine placerRoutine;
	SpanningTreeEnforcingRoutine spanningTreeEnforcingRoutine;
	GraphReintegrationRoutine reintegrationRoutine;
	AdaptationStrategy adaptationStrategy;

	boolean merge;
	boolean place;
	boolean layout;
	boolean reintegrate;
	boolean preparePlacer;
	boolean spanningTreeSetup;
	boolean enforceSPTree;
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

		spanningTreeEnforcingRoutine = new SpanningTreeEnforcingRoutine();
		spanningTreeEnforcingRoutine.initialize(this);

		merge = false;
		layout = false;
		reintegrate = false;
		spanningTreeSetup = false;
		preparePlacer = false;
		place = false;
		enforceSPTree = false;
		terminate = false;

		workers = GiraphConstants.SPLIT_MASTER_WORKER.get(getConf()) ? getConf().getMapTasks() - 1 : getConf().getMapTasks();

		try {
			Class< ? extends AdaptationStrategy> tClass =
					(Class< ? extends AdaptationStrategy>) Class.forName(getConf()
							.getStrings(adaptationStrategyString,
									SizeAndDiameterDrivenAdaptationStrategy.class.toString())[0]);
			adaptationStrategy = tClass.getConstructor(String.class, GiraphConfiguration.class).newInstance(getConf().getStrings(adaptationStrategyOptionsString, "")[0], getConf());
		} catch (Exception e) {
			log.info("Caught exception, switching to default");
			adaptationStrategy = new SizeDiameterEnvironmentAwareDrivenAdaptationStrategy(getConf().getStrings(adaptationStrategyOptionsString, "")[0], getConf());
		}

		registerPersistentAggregator(thresholdSurpassedAggregator, BooleanAndAggregator.class);

	}

	public void compute() {
		if (getSuperstep() == 0) {
			if(!getConf().getBoolean(skipMergingString, false))
				merge = true;
			else{
				merge = false;
				spanningTreeSetup = true;
				setComputation(SolarMerger.StateRestore.class);
				return;	
			}
		}

		int currentLayer =
				((IntWritable) getAggregatedValue(SolarMergerRoutine.currentLayerAggregator))
				.get();

		if (merge) {
			if (!mergerRoutine.compute()) {
				return;
			}
			else {				
				merge = false;
				spanningTreeSetup = true;							
			}
		}

		int noOfVertices =
				((IntWritable) ((MapWritable) getAggregatedValue(SolarMergerRoutine.layerVertexSizeAggregator)) //HERE!
						.get(new IntWritable(currentLayer))).get();
		int noOfLayers =
				((IntWritable) getAggregatedValue(SolarMergerRoutine.layerNumberAggregator))
				.get();
		int noOfEdges =
				((IntWritable) ((MapWritable) getAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator))
						.get(new IntWritable(currentLayer))).get();

		//CORRECT NO OF EDGES PER LAYER IN CASE OF RESTORE OF COMPUTATION
		if(getComputation().equals(SolarMerger.StateRestore.class)){			
			MapWritable rawEdges = ((MapWritable) getAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator));
			Iterator<Entry<Writable, Writable>> itE = rawEdges.entrySet().iterator();
			while(itE.hasNext()){
				Entry<Writable, Writable> current = itE.next();
				int cLayer = ((IntWritable)current.getKey()).get();				
				int eSize = ((IntWritable)current.getValue()).get()/2;
				rawEdges.put(current.getKey(), new IntWritable(eSize));
				getContext().getCounter(SolarMergerRoutine.COUNTER_GROUP, "Layer " + (cLayer) + " edges").increment(eSize);				
			}
			MapWritable rawVertices = ((MapWritable) getAggregatedValue(SolarMergerRoutine.layerVertexSizeAggregator));
			Iterator<Entry<Writable, Writable>> itV = rawVertices.entrySet().iterator();
			while(itV.hasNext()){
				Entry<Writable, Writable> current = itV.next();
				int cLayer = ((IntWritable)current.getKey()).get();
				int vSize = ((IntWritable)current.getValue()).get();
				getContext().getCounter(SolarMergerRoutine.COUNTER_GROUP, "Layer " + (cLayer) + " vertices").increment(vSize);
			}
			setAggregatedValue(SolarMergerRoutine.layerEdgeSizeAggregator, rawEdges);

			noOfLayers = currentLayer + 1;
			setAggregatedValue(SolarMergerRoutine.layerNumberAggregator, new IntWritable(noOfLayers));
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

		if (preparePlacer) {
			preparePlacer = false;
			updateCountersAndAggregators(currentLayer, noOfLayers, noOfVertices,
					noOfEdges);

			if(!getConf().getBoolean(skipMergingString, false)){
				layout = true;				
			}else
				if (currentLayer > 0) {
					place = true;
				}
				else {
					reintegrate = true;
				}
		}

		if (currentLayer >= 0 && !reintegrate) {

			if(place || enforceSPTree){
				if (place)
					if (!placerRoutine.compute())
						return;
					else {
						place = false;
						resetLayoutAggregators();
						boolean thresholdState = updateCountersAndAggregators(currentLayer, noOfLayers, noOfVertices,
								noOfEdges);
						if(thresholdState)
							layout = true;
						else
							enforceSPTree = true;
					}
				
				if(enforceSPTree)				
					if (!spanningTreeEnforcingRoutine.compute())
						return;
					else {
						enforceSPTree = false;
						layout = true;
					}
			}

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
					if(getConf().getBoolean(mergeOnlyString, false)){
						haltComputation();
						return;
					}
					if (currentLayer > 0) {
						place = true;
					}
					else {
						reintegrate = true;
					}

				}
			}

		}
		if (reintegrate)
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

	private boolean updateCountersAndAggregators(int currentLayer, int noOfLayers,
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
				adaptationStrategy.returnTargetAccuracy(currentLayer, noOfLayers,
						noOfVertices, noOfEdges);
		boolean thresholdState = adaptationStrategy.thresholdSurpassed();
		
		String layerText = "Layer " + currentLayer;

		setAggregatedValue(LayoutRoutine.ttlMaxAggregator, new IntWritable(
				selectedK));
		setAggregatedValue(LayoutRoutine.coolingSpeedAggregator,
				new DoubleWritable(coolingSpeed));
		setAggregatedValue(LayoutRoutine.initialTempFactorAggregator,
				new DoubleWritable(initialTemp));
		setAggregatedValue(LayoutRoutine.currentAccuracyAggregator,
				new DoubleWritable(accuracy));
		setAggregatedValue(thresholdSurpassedAggregator, new BooleanWritable(thresholdState));

		getContext().getCounter("Layer Counters", layerText + " k")
		.increment(selectedK);
		getContext().getCounter("Layer Counters",
				layerText + " coolingSpeed").increment(
						(long) (coolingSpeed * 100));
		getContext().getCounter("Layer Counters",
				layerText + " tempFactor").increment(
						(long) (initialTemp * 100));
		getContext().getCounter("Layer Counters",
				layerText + " accuracy").increment(
						(long) (accuracy * 100000));

		return thresholdState;
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
