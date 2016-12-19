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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;

import com.google.common.collect.Lists;

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
   * @param p1
   *          The first point.
   * @param p2
   *          The second point.
   * @return The square distance.
   */
  public static float floatSquareModule(float[] p1, float[] p2) {
    float result = new Double(Math.pow(p2[0] - p1[0], 2) + Math.pow(p2[1]
            - p1[1], 2)).floatValue();
    return floatFuzzyMath(result);
  }
  
  /**
   * A method to compute the square distance between two points.
   * 
   * @param p1
   *          The first point.
   * @param p2
   *          The second point.
   * @return The square distance.
   */
  public static double squareModule(double[] p1, double[] p2) {
    double result = Math.pow(p2[0] - p1[0], 2) + Math.pow(p2[1]
            - p1[1], 2);
    return doubleFuzzyMath(result);
  }
  
  /**
   * This method computes the square root of the square distance.
   * 
   * @param p1
   * @param p2
   * @return The square rooted distance between two points.
   */
  public static double floatComputeModule(float[] p1, float[] p2) {   
    return floatFuzzyMath(new Double(Math.sqrt(floatSquareModule(p1, p2))).floatValue());
  }

  /**
   * This method computes the square root of the square distance.
   * 
   * @param p1
   * @param p2
   * @return The square rooted distance between two points.
   */
  public static double computeModule(double[] p1, double[] p2) {   
    return doubleFuzzyMath(Math.sqrt(squareModule(p1, p2)));
  }

  /**
   * A simple method to compute the module of a vector of size 2.
   * 
   * @param vector
   *          The vector whose module is requested.
   * @return The requested module.
   */
  public static double computeModule(double[] vector) {
    return doubleFuzzyMath(Math.sqrt((Math.pow(vector[0], 2) + Math
            .pow(vector[1], 2))));
  }
  
  /**
   * This method ensures that the given value is not equal to 0, returning the
   * same given value if it is not equal to zero or a very small value
   * otherwise.
   * 
   * @param value
   * @return The value itself or a very small positive value otherwise.
   */
  public static float floatFuzzyMath(float value) {
    if (value == 0.0)
      return new Float(0.00001);
    return value;
  }

  /**
   * This method ensures that the given value is not equal to 0, returning the
   * same given value if it is not equal to zero or a very small value
   * otherwise.
   * 
   * @param value
   * @return The value itself or a very small positive value otherwise.
   */
  public static double doubleFuzzyMath(double value) {
    if (value == 0.0)
      return new Double(0.00001);
    return value;
  }

  public static <V extends CoordinateWritable, E extends Writable> List<Double> buildSlopesList(
          Iterator<LayoutMessage> its,
          Vertex<LayeredPartitionedLongWritable, V, E> vertex)
          throws IOException {
    LinkedList<Double> tempList = new LinkedList<Double>();
    double[] myCoordinates = vertex.getValue().getCoordinates();
    while (its.hasNext()) {
      LayoutMessage currentNeighbour = its.next();
      double[] coordinates = currentNeighbour.getValue();
      vertex.getValue().setShortestEdge(
              Toolbox.computeModule(myCoordinates, coordinates));
      double computedAtan = Math.atan2(coordinates[1] - myCoordinates[1],
              coordinates[0] - myCoordinates[0]);
      tempList.add(computedAtan);
    }
    return tempList;
  }

  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
          Map<K, V> map, boolean ascending) {
    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
            map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        return (o1.getValue()).compareTo(o2.getValue());
      }
    });
    if (!ascending)
      Lists.reverse(list);
    Map<K, V> result = new LinkedHashMap<K, V>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

}
