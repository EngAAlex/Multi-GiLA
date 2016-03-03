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
package unipg.gila.layout;

import java.awt.geom.Line2D;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.utils.Toolbox;

/**
 * This class holds the computations and support methods needed to reintegrate the previously pruned one degree vertices into the graph.
 * 
 * @author Alessio Arleo
 *
 */
public class GraphReintegration {
		
	/**
	 * A plain dummy computation used to propagate the vertices reintegration.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class PlainDummyComputation extends AbstractComputation<PartitionedLongWritable, CoordinateWritable, NullWritable, LayoutMessage, LayoutMessage> {

		@Override
		public void compute(
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			return;
		}

	}

	/**
	 * This reintegration method simply arranges the one degree vertices around their neighbor.
	 * @author Alessio Arleo
	 *
	 */
	public static class RadialReintegrateOneEdges extends PlainGraphReintegration{

		@Override
		public void compute(
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			int size = vertex.getValue().getOneDegreeVerticesQuantity();
			if(size == 0)
				return;
			float[][] verticesCollection = computeOneDegreeVerticesCoordinates(vertex, size, new Double(Math.PI*2).floatValue(), 0.0f);
			reconstructGraph(verticesCollection, vertex);
		}

		@Override
		public void preSuperstep() {
			super.preSuperstep();
		}

	}

	/**
	 * This class places the one degree vertices in a cone. The bisector is the direction of the resulting force vector acting on the 
	 * neighbor and the angular width is given by "reintegration.coneWidth" option.
	 * 
	 * @author general
	 *
	 */
	public static class ConeReintegrateOneEdges extends PlainGraphReintegration{

		@Override
		public void compute(
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			int size = vertex.getValue().getOneDegreeVerticesQuantity();
			if(size == 0)
				return;
			reconstructGraph(super.placeVerticesInCone(vertex, size, messages), vertex);

		}

		@Override
		public void preSuperstep() {
			super.preSuperstep();
			aperture = new Double(getConf().getFloat(FloodingMaster.coneWidth, FloodingMaster.coneWidthDefault) * DEGREE_TO_RADIANS_CONSTANT).floatValue();
		}

	}

	/**
	 * This class reintegrates all the one degree vertices of a vertex in the widest angle formed by its neighbors.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class MaxSlopeReintegrateOneDegrees extends PlainGraphReintegration{

		private float maxSlope;
		float selectedStart = 0;

		@Override
		public void compute(
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {	
			int size = vertex.getValue().getOneDegreeVerticesQuantity();			
			if(size == 0)
				return;
			computePositions(size, vertex, messages);
		}

		protected void computePositions(int size, Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex, Iterable<LayoutMessage> itl) throws IOException{
			maxSlope = Float.MIN_VALUE;
			float[] myCoordinates = vertex.getValue().getCoordinates();
			Iterator<LayoutMessage> its = itl.iterator();		
			if(vertex.getNumEdges() == 0){ 
				float[][] verticesCollection = computeOneDegreeVerticesCoordinates(vertex, size, new Double(Math.PI*2).floatValue(), 0);
				reconstructGraph(verticesCollection, vertex);
				return;
			}else if(vertex.getNumEdges() == 1){
				LayoutMessage currentNeighbour = its.next();
				float[] coordinates = currentNeighbour.getValue();
				float computedAtan = (float) Math.atan2(coordinates[1] - myCoordinates[1], coordinates[0] - myCoordinates[0]);
				reconstructGraph(computeOneDegreeVerticesCoordinates(vertex, size, (float)Math.PI*2, 
						computedAtan), vertex);
				return;
			}
			boolean firstTime = true;
			Double lastSlope = 0.0;
			LinkedList<Double> tempSlopesList = buildSlopesList(its, vertex); 
			Collections.sort(tempSlopesList);
			Iterator<Double> it = tempSlopesList.iterator();
			int counter = 0;
			float pie = 0.0f;
			while(it.hasNext() || counter < tempSlopesList.size()){
				Double currentSlope = 0.0;
				try{
					currentSlope = it.next();
				}catch(NoSuchElementException nsfw){
					storeInformation(new Double(Math.PI*2 - pie).floatValue(), new Double(lastSlope).floatValue());
					counter++;				
					continue;
				}if(firstTime){
					firstTime = false;
					lastSlope = currentSlope;
					continue;
				}
				float slope = new Double(Math.abs(currentSlope - lastSlope)).floatValue();
				pie += slope;
				storeInformation(slope, new Double(lastSlope).floatValue());
				lastSlope = currentSlope;
				counter++;
			}
			rebuild(size, vertex);
		}

		private LinkedList<Double> buildSlopesList(Iterator<LayoutMessage> its,
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex) throws IOException {
			LinkedList<Double> tempList = new LinkedList<Double>();
			float[] myCoordinates = vertex.getValue().getCoordinates();
			while(its.hasNext()){
				LayoutMessage currentNeighbour = its.next();
				float[] coordinates = currentNeighbour.getValue();
				vertex.getValue().setShortestEdge(Toolbox.computeModule(myCoordinates, coordinates));
				double computedAtan = Math.atan2(coordinates[1] - myCoordinates[1], coordinates[0] - myCoordinates[0]);
				tempList.add(computedAtan);
			}
			return tempList;
		}

		protected void rebuild(int size, Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex) throws IOException {
			float[][] verticesToPlace = computeOneDegreeVerticesCoordinates(vertex, size, maxSlope, selectedStart);
			reconstructGraph(verticesToPlace, vertex);			
		}


		protected void storeInformation(float slope,
				float startSlope){
			if(slope - padding*2 > maxSlope){
				maxSlope = slope  - padding*2;
				selectedStart = startSlope + padding;
			}			
		}

		public void preSuperstep() {
			super.preSuperstep();
			padding = new Double(getConf().getFloat(FloodingMaster.paddingString, paddingDefault)*DEGREE_TO_RADIANS_CONSTANT).floatValue();
		}
		
	}

	/**
	 * This class fairly distributes the one degree vertices of each vertex arranging them in the angles formed by the
	 * vertex neighbors. If the angle is below the value given by the "reintegration.fairLowThreshold" option (expressed in degrees)
	 * that angle is skipped and the vertices arranged in the other gaps.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class FairShareReintegrateOneEdges extends MaxSlopeReintegrateOneDegrees{

		protected HashMap<Float, Float> slopes; 	
		protected float lowThreshold;

		@Override
		public void compute(
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
				Iterable<LayoutMessage> messages) throws IOException {
			slopes = new HashMap<Float, Float>();
			super.compute(vertex, messages);
		}

		@Override
		protected void storeInformation(float slope,
				float startSlope){
			if(slope >= lowThreshold + padding*2){
				float modifiedSlope = slope;
				while(slopes.containsKey(modifiedSlope)){
					modifiedSlope = new Double(slope + Math.random()/2).floatValue();
				}
				slopes.put(modifiedSlope, startSlope + padding);
			}
		}

		@Override
		protected void rebuild(
				int size,
				Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex) throws IOException {
			if(slopes.size() == 0){
				float[][] verticesToPlace = computeOneDegreeVerticesCoordinates(vertex, size, 0, 0);
				reconstructGraph(verticesToPlace, vertex);
				return;
			}
			Iterator<Entry<Float, Float>> it = slopes.entrySet().iterator();
			Iterator<LongWritable> oneDegreesIterator = vertex.getValue().getOneDegreeVertices();
			int remaining = size;
						
			while(it.hasNext() && remaining > 0){
				Entry<Float, Float> current = it.next();
				int quantity = 0;
				if(!it.hasNext()){
					quantity = remaining;
				}else{
					quantity = new Double(Math.round(remaining * ((current.getKey()/(Math.PI*2)) % 1.0))).intValue();
				}			
				reconstructGraphKeepingIterator(computeOneDegreeVerticesCoordinates(vertex, quantity, current.getKey(), current.getValue()), vertex, oneDegreesIterator);
				remaining -= quantity;
			}
			if(oneDegreesIterator.hasNext())
				throw new IOException("OneEdges iterator was not fully explored.");
		}


		public void preSuperstep() {
			super.preSuperstep();	
			lowThreshold = new Double(getConf().getFloat(FloodingMaster.lowThresholdString, FloodingMaster.lowThresholdDefault)*DEGREE_TO_RADIANS_CONSTANT).floatValue();
		}
	}

	/**
	 * Abstract class that holds base methods to reintegrate the vertices.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static abstract class PlainGraphReintegration extends AbstractComputation<PartitionedLongWritable, CoordinateWritable, NullWritable, LayoutMessage, LayoutMessage>{

		protected static final double DEGREE_TO_RADIANS_CONSTANT = Math.PI/180;
		protected static final double RADIANS_TO_DEGREE_CONSTANT = 180/Math.PI;		

		protected float paddingDefault = 2.0f;
		protected float padding;

		protected long added;

		protected boolean isRadiusDynamic;

		protected float aperture; 

		protected float radius;
		protected float k;

		protected float[][] vertexPlacer(long quantity, float sector, float start, float[] myCoords, float effectiveRadius) throws IOException{
			if(Float.isNaN(sector) || Float.isNaN(start) || Float.isNaN(effectiveRadius) || Float.isNaN(myCoords[0]) || Float.isNaN(myCoords[1])){
				throw new IOException("NaN alert OMEGA " + sector + " " + start + " " + effectiveRadius + " " + myCoords[0] + " " + myCoords[1]);
			}
			if(quantity < 0)
 				return new float[0][0];
			float[][] result = new float[new Long(quantity).intValue()][2];
			double angularResolution = sector/(double)quantity;
			double halfAngularResolution = angularResolution/2;
			for(int i=0; i<quantity; i++){
				double currentDeg = start + halfAngularResolution + i*angularResolution;
				float x = myCoords[0] + new Double(Math.cos(currentDeg)).floatValue()*effectiveRadius;
				float y = myCoords[1] + new Double(Math.sin(currentDeg)).floatValue()*effectiveRadius;
				result[i][0] = x;
				result[i][1] = y;
			}
			return result;
		}
		
		private float[][] placeVerticesInGap(int quantity, float sector, float start, float[] myCoords) throws IOException{
			return vertexPlacer(quantity, sector, start, myCoords, radius * k);
		}

		private float[][] placeVerticesInGapWithShortestEdge(long quantity, float sector, float start, float[] myCoords, float shortestEdge) throws IOException{
			return vertexPlacer(quantity, sector, start, myCoords, radius * shortestEdge);
		}

		protected float[][] computeOneDegreeVerticesCoordinates(Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex, int quantity, float slopeAngle, float slopeStart) throws IOException{
			float[][] placedVertices;
			float shortestEdge = vertex.getValue().getShortestEdge();
			if(isRadiusDynamic){
				if(shortestEdge == Float.MAX_VALUE){
					placedVertices = placeVerticesInGap(quantity, slopeAngle, slopeStart, vertex.getValue().getCoordinates());
				}else{
					placedVertices = placeVerticesInGapWithShortestEdge(quantity, slopeAngle, slopeStart, vertex.getValue().getCoordinates(), shortestEdge);
				}
			}else{
				placedVertices = placeVerticesInGap(quantity, slopeAngle, slopeStart, vertex.getValue().getCoordinates());
			}
			return placedVertices;
		}

		protected void reconstructGraph(float[][] verticesToPlace, Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex) throws IOException{
			Iterator<LongWritable> oneDegreesIt = vertex.getValue().getOneDegreeVertices();
			for(int i=0; i<verticesToPlace.length; i++){
				addSingleOneDegreeVertex(oneDegreesIt.next().get(), verticesToPlace[i], vertex);
			}
			if(oneDegreesIt.hasNext())
				throw new IOException("One Edges iterator was not completely explored");
		}
		
		protected void reconstructGraphKeepingIterator(float[][] verticesToPlace, Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex, Iterator<LongWritable> woundIterator) throws IOException{
			for(int i=0; i<verticesToPlace.length; i++){
				addSingleOneDegreeVertex(woundIterator.next().get(), verticesToPlace[i], vertex);
			}
		}
		
		private void addSingleOneDegreeVertex(long idOfOneEdge, float[] coordinatesOfVertexToPlace, Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> neighborVertex){
			ArrayListEdges<PartitionedLongWritable, NullWritable> ale = new ArrayListEdges<PartitionedLongWritable, NullWritable>();
			ale.initialize(1);
			ale.add(EdgeFactory.create(neighborVertex.getId(), NullWritable.get()));
			CoordinateWritable value = new CoordinateWritable(coordinatesOfVertexToPlace[0], coordinatesOfVertexToPlace[1], neighborVertex.getValue().getComponent());
			PartitionedLongWritable oE = new PartitionedLongWritable(neighborVertex.getId().getPartition(), idOfOneEdge);
			added++;
			try {
				addVertexRequest(oE, value, ale);
				addEdgeRequest(neighborVertex.getId(), EdgeFactory.create(oE, NullWritable.get()));					
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		protected float[][] placeVerticesInCone(Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex, int size, Iterable<LayoutMessage> itl) throws IOException{
			float[] statusCopy = vertex.getValue().getCoordinates();			
			float[] force = new float[]{0.0f, 0.0f};

			//			CoordinateWritable<Float> vValue = vertex.getValue();
			Iterator<LayoutMessage> cohords = itl.iterator();

			while(cohords.hasNext()){

				LayoutMessage val = cohords.next();

				float[] foreignCoordinates = val.getValue();

				float distanceFromVertex = Toolbox.computeModule(statusCopy, new float[]{foreignCoordinates[0], foreignCoordinates[1]});

				float inverseSquareDist = new Double(1/Math.pow(distanceFromVertex,2)).floatValue();

				force[0] += (statusCopy[0] - foreignCoordinates[0])*inverseSquareDist;
				force[1] += (statusCopy[1] - foreignCoordinates[1])*inverseSquareDist;
			}

			float theta = new Double(Math.atan2(force[1], force[0])).floatValue();
			float start = theta-aperture/2;

			float[][] verticesToPlace = placeVerticesInGap(size, aperture, start, vertex.getValue().getCoordinates());
			return verticesToPlace;
		}

		@Override
		public void preSuperstep() {
			added=0;
			k = ((FloatWritable)getAggregatedValue(FloodingMaster.k_agg)).get();
			radius = getConf().getFloat(FloodingMaster.radiusString, FloodingMaster.radiusDefault);
			isRadiusDynamic = getConf().getBoolean(FloodingMaster.dynamicRadiusString, true);
		}
	}

	public static double angleBetween2Lines(Line2D line1, Line2D line2)
	{
		double angle1 = Math.atan2(line1.getY1() - line1.getY2(),
				line1.getX1() - line1.getX2());
		double angle2 = Math.atan2(line2.getY1() - line2.getY2(),
				line2.getX1() - line2.getX2());
		double result = angle1-angle2;
		if(result < 0)
			result = Math.PI*2 - Math.abs(result);
		return result;
	}

}

