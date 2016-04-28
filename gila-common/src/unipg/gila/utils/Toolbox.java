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
package unipg.gila.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;


/**
 * This class implements a few convenience methods.
 * 
 * @author Alessio Arleo
 *
 */
public class Toolbox { 

	/**
	 * A method to compute the square distance between two points.
	 * 
	 * @param p1 The first point.
	 * @param p2 The second point.
	 * @return The square distance.
	 */
	public static float squareModule(float[] p1, float[] p2){
		float result = (float) (Math.pow(p2[0] - p1[0],2) + Math.pow(p2[1] - p1[1], 2));
		return floatFuzzyMath(result);
	}

	/**
	 * This method computes the square root of the square distance.
	 * 
	 * @param p1
	 * @param p2
	 * @return The square rooted distance between two points.
	 */
	public static float computeModule(float[] p1, float[] p2){
		float result = (float) Math.sqrt(squareModule(p1, p2));
		return floatFuzzyMath(result);
	}


	/**
	 * A simple method to compute the module of a vector of size 2.
	 * 
	 * @param vector The vector whose module is requested.
	 * @return The requested module.
	 */
	public static float computeModule(float[] vector) {
		return floatFuzzyMath(new Float(Math.sqrt((Math.pow(vector[0], 2) + Math.pow(vector[1], 2)))));
	}


	/**
	 * This method ensures that the given value is not equal to 0, returning the same given value if it is not equal to zero
	 * or a very small value otherwise.
	 * @param value
	 * @return The value itself or a very small positive value otherwise.
	 */
	public static float floatFuzzyMath(float value){
		if(value == 0)
			return new Float(0.00001);
		return value;
	}

	public static <V extends CoordinateWritable, E extends Writable> List<Float> buildSlopesList(Iterator<LayoutMessage> its,
			Vertex<LayeredPartitionedLongWritable, V, E> vertex) throws IOException {
		LinkedList<Float> tempList = new LinkedList<Float>();
		float[] myCoordinates = vertex.getValue().getCoordinates();
		while(its.hasNext()){
			LayoutMessage currentNeighbour = its.next();
			float[] coordinates = currentNeighbour.getValue();
			vertex.getValue().setShortestEdge(Toolbox.computeModule(myCoordinates, coordinates));
			double computedAtan = Math.atan2(coordinates[1] - myCoordinates[1], coordinates[0] - myCoordinates[0]);
			tempList.add(new Float(computedAtan));
		}
		return tempList;
	}

	public static <V extends CoordinateWritable, E extends Writable> HashMap<LayeredPartitionedLongWritable, Float> buildSlopesMap(Iterator<LayoutMessage> its,
			Vertex<LayeredPartitionedLongWritable, V, E> vertex) throws IOException {
		HashMap<LayeredPartitionedLongWritable, Float> tempList = new HashMap<LayeredPartitionedLongWritable, Float>();
		float[] myCoordinates = vertex.getValue().getCoordinates();
		while(its.hasNext()){
			LayoutMessage currentNeighbour = its.next();
			float[] coordinates = currentNeighbour.getValue();
//			vertex.getValue().setShortestEdge(Toolbox.computeModule(myCoordinates, coordinates));
			double computedAtan = Math.atan2(coordinates[1] - myCoordinates[1], coordinates[0] - myCoordinates[0]);
			computedAtan = computedAtan < 0 ? (Math.PI*2 + computedAtan) : computedAtan;
			tempList.put(currentNeighbour.getPayloadVertex(), new Float(computedAtan));
		}
		return tempList;
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValue( Map<K, V> map )
	{
		List<Map.Entry<K, V>> list =
				new LinkedList<Map.Entry<K, V>>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
				{
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
			{
				return (o1.getValue()).compareTo( o2.getValue() );
			}
				} );

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list)
		{
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}

}
