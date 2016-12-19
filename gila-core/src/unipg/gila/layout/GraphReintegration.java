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
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.utils.Toolbox;

/**
 * This class holds the computations and support methods needed to reintegrate
 * the previously pruned one degree vertices into the graph.
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
  public static class PlainDummyComputation<V extends CoordinateWritable, E extends Writable>
          extends
          AbstractComputation<LayeredPartitionedLongWritable, V, E, LayoutMessage, LayoutMessage> {

    @Override
    public void compute(Vertex<LayeredPartitionedLongWritable, V, E> vertex,
            Iterable<LayoutMessage> messages) throws IOException {
      return;
    }

  }

  /**
   * This reintegration method simply arranges the one degree vertices around
   * their neighbor.
   * 
   * @author Alessio Arleo
   *
   */
  public static class RadialReintegrateOneEdges<V extends CoordinateWritable, E extends Writable>
          extends PlainGraphReintegration<V, E> {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph
     * .graph.Vertex, java.lang.Iterable)
     */
    @Override
    public void compute(Vertex<LayeredPartitionedLongWritable, V, E> vertex,
            Iterable<LayoutMessage> messages) throws IOException {
      int size = vertex.getValue().getOneDegreeVerticesQuantity();
      if (size == 0)
        return;
      double[][] verticesCollection = computeOneDegreeVerticesCoordinates(
              vertex, size, Math.PI * 2, 0.0f);
      reconstructGraph(verticesCollection, vertex);
    }

    @Override
    public void preSuperstep() {
      super.preSuperstep();
    }

  }

  /**
   * This class places the one degree vertices in a cone. The bisector is the
   * direction of the resulting force vector acting on the neighbor and the
   * angular width is given by "reintegration.coneWidth" option.
   * 
   * @author general
   *
   */
  public static class ConeReintegrateOneEdges<V extends CoordinateWritable, E extends Writable>
          extends PlainGraphReintegration<V, E> {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph
     * .graph.Vertex, java.lang.Iterable)
     */
    @Override
    public void compute(Vertex<LayeredPartitionedLongWritable, V, E> vertex,
            Iterable<LayoutMessage> messages) throws IOException {
      int size = vertex.getValue().getOneDegreeVerticesQuantity();
      if (size == 0)
        return;
      reconstructGraph(super.placeVerticesInCone(vertex, size, messages),
              vertex);
    }

    @Override
    public void preSuperstep() {
      super.preSuperstep();
      aperture = getConf().getDouble(LayoutRoutine.coneWidth,
              LayoutRoutine.coneWidthDefault)
              * DEGREE_TO_RADIANS_CONSTANT;
    }

  }

  /**
   * This class reintegrates all the one degree vertices of a vertex in the
   * widest angle formed by its neighbors.
   * 
   * @author Alessio Arleo
   *
   */
  public static class MaxSlopeReintegrateOneDegrees<V extends CoordinateWritable, E extends Writable>
          extends PlainGraphReintegration<V, E> {

    private double maxSlope;
    double selectedStart = 0;

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.giraph.graph.AbstractComputation#compute(org.apache.giraph
     * .graph.Vertex, java.lang.Iterable)
     */
    @Override
    public void compute(Vertex<LayeredPartitionedLongWritable, V, E> vertex,
            Iterable<LayoutMessage> messages) throws IOException {
      int size = vertex.getValue().getOneDegreeVerticesQuantity();
      if (size == 0)
        return;
      computePositions(size, vertex, messages);
    }

    protected void computePositions(int size,
            Vertex<LayeredPartitionedLongWritable, V, E> vertex,
            Iterable<LayoutMessage> itl) throws IOException {
      maxSlope = Double.MIN_VALUE;
      double[] myCoordinates = vertex.getValue().getCoordinates();
      Iterator<LayoutMessage> its = itl.iterator();
      List<Double> tempSlopesList = Toolbox.buildSlopesList(its, vertex);
      its = itl.iterator();
      if (vertex.getNumEdges() == 0) {
        double[][] verticesCollection = computeOneDegreeVerticesCoordinates(
                vertex, size, Math.PI * 2, 0);
        reconstructGraph(verticesCollection, vertex);
        return;
      } else if (vertex.getNumEdges() == 1) {
        LayoutMessage currentNeighbour = its.next();
        double[] coordinates = currentNeighbour.getValue();
        double computedAtan = Math.atan2(coordinates[1]
                - myCoordinates[1], coordinates[0] - myCoordinates[0]);
        reconstructGraph(
                computeOneDegreeVerticesCoordinates(vertex, size,
                        Math.PI * 2, computedAtan), vertex);
        return;
      }
      boolean firstTime = true;
      double lastSlope = 0.0f;
      Collections.sort(tempSlopesList);
      Iterator<Double> it = tempSlopesList.iterator();
      int counter = 0;
      double pie = 0.0;
      while (it.hasNext() || counter < tempSlopesList.size()) {
        double currentSlope = 0.0f;
        try {
          currentSlope = it.next();
        } catch (NoSuchElementException nsfw) {
          storeInformation(Math.PI * 2 - pie,
                  lastSlope);
          counter++;
          continue;
        }
        if (firstTime) {
          firstTime = false;
          lastSlope = currentSlope;
          continue;
        }
        double slope = Math.abs(currentSlope - lastSlope);                
        pie += slope;
        storeInformation(slope, lastSlope);
        lastSlope = currentSlope;
        counter++;
      }
      rebuild(size, vertex);
    }

    protected void rebuild(int size,
            Vertex<LayeredPartitionedLongWritable, V, E> vertex)
            throws IOException {
      double[][] verticesToPlace = computeOneDegreeVerticesCoordinates(vertex,
              size, maxSlope, selectedStart);
      reconstructGraph(verticesToPlace, vertex);
    }

    protected void storeInformation(double slope, double startSlope) {
      if (slope - padding * 2 > maxSlope) {
        maxSlope = slope - padding * 2;
        selectedStart = startSlope + padding;
      }
    }

    public void preSuperstep() {
      super.preSuperstep();
      padding = getConf().getDouble(LayoutRoutine.paddingString,
              paddingDefault) * DEGREE_TO_RADIANS_CONSTANT;
    }

  }

  /**
   * This class fairly distributes the one degree vertices of each vertex
   * arranging them in the angles formed by the vertex neighbors. If the angle
   * is below the value given by the "reintegration.fairLowThreshold" option
   * (expressed in degrees) that angle is skipped and the vertices arranged in
   * the other gaps.
   * 
   * @author Alessio Arleo
   *
   */
  public static class FairShareReintegrateOneEdges<V extends CoordinateWritable, E extends Writable>
          extends MaxSlopeReintegrateOneDegrees<V, E> {

    protected HashMap<Double, Double> slopes;
    protected double lowThreshold;

    /*
     * (non-Javadoc)
     * 
     * @see
     * unipg.gila.layout.GraphReintegration.MaxSlopeReintegrateOneDegrees#compute
     * (org.apache.giraph.graph.Vertex, java.lang.Iterable)
     */
    @Override
    public void compute(Vertex<LayeredPartitionedLongWritable, V, E> vertex,
            Iterable<LayoutMessage> messages) throws IOException {
      slopes = new HashMap<Double, Double>();
      super.compute(vertex, messages);
    }

    @Override
    protected void storeInformation(double slope, double startSlope) {
      if (slope >= lowThreshold + padding * 2) {
        double modifiedSlope = slope;
        while (slopes.containsKey(modifiedSlope)) {
          modifiedSlope = slope + Math.random() / 2;
        }
        slopes.put(modifiedSlope, startSlope + padding);
      }
    }

    @Override
    protected void rebuild(int size,
            Vertex<LayeredPartitionedLongWritable, V, E> vertex)
            throws IOException {
      if (slopes.size() == 0) {
        double[][] verticesToPlace = computeOneDegreeVerticesCoordinates(
                vertex, size, 0, 0);
        reconstructGraph(verticesToPlace, vertex);
        return;
      }
      Iterator<Entry<Double, Double>> it = slopes.entrySet().iterator();
      Iterator<LongWritable> oneDegreesIterator = vertex.getValue()
              .getOneDegreeVertices();
      int remaining = size;

      while (it.hasNext() && remaining > 0) {
        Entry<Double, Double> current = it.next();
        int quantity = 0;
        if (!it.hasNext()) {
          quantity = remaining;
        } else {
          quantity = new Double(Math.round(remaining
                  * ((current.getKey() / (Math.PI * 2)) % 1.0))).intValue();
        }
        reconstructGraphKeepingIterator(
                computeOneDegreeVerticesCoordinates(vertex, quantity,
                        current.getKey(), current.getValue()), vertex,
                oneDegreesIterator);
        remaining -= quantity;
      }
      if (oneDegreesIterator.hasNext())
        throw new IOException("OneEdges iterator was not fully explored.");
    }

    public void preSuperstep() {
      super.preSuperstep();
      lowThreshold = getConf().getDouble(
              LayoutRoutine.lowThresholdString,
              LayoutRoutine.lowThresholdDefault)
              * DEGREE_TO_RADIANS_CONSTANT;
    }
  }

  /**
   * Abstract class that holds base methods to reintegrate the vertices.
   * 
   * @author Alessio Arleo
   *
   */
  public static abstract class PlainGraphReintegration<V extends CoordinateWritable, E extends Writable>
          extends
          AbstractComputation<LayeredPartitionedLongWritable, V, E, LayoutMessage, LayoutMessage> {

    protected static final double DEGREE_TO_RADIANS_CONSTANT = Math.PI / 180;
    protected static final double RADIANS_TO_DEGREE_CONSTANT = 180 / Math.PI;

    protected double paddingDefault = 2.0;
    protected double padding;

    protected long added;

    protected boolean isRadiusDynamic;

    protected double aperture;

    protected double radius;
    protected double k;

    protected double[][] vertexPlacer(long quantity, double sector, double start,
      double[] myCoords, double effectiveRadius) throws IOException {
      if (Double.isNaN(sector) || Double.isNaN(start)
              || Double.isNaN(effectiveRadius) || Double.isNaN(myCoords[0])
              || Double.isNaN(myCoords[1])) {
        throw new IOException("NaN alert OMEGA " + sector + " " + start + " "
                + effectiveRadius + " " + myCoords[0] + " " + myCoords[1]);
      }
      if (quantity < 0)
        return new double[0][0];
      double[][] result = new double[new Long(quantity).intValue()][2];
      double angularResolution = sector / (double) quantity;
      double halfAngularResolution = angularResolution / 2;
      for (int i = 0; i < quantity; i++) {
        double currentDeg = start + halfAngularResolution + i
                * angularResolution;
        double x = myCoords[0] + Math.cos(currentDeg) * effectiveRadius;
        double y = myCoords[1] + Math.sin(currentDeg) * effectiveRadius;
        result[i][0] = x;
        result[i][1] = y;
      }
      return result;
    }

    private double[][] placeVerticesInGap(int quantity, double sector,
      double start, double[] myCoords) throws IOException {
      return vertexPlacer(quantity, sector, start, myCoords, radius * k);
    }

    private double[][] placeVerticesInGapWithShortestEdge(long quantity,
      double sector, double start, double[] myCoords, double shortestEdge)
            throws IOException {
      return vertexPlacer(quantity, sector, start, myCoords, radius
              * shortestEdge);
    }

    protected double[][] computeOneDegreeVerticesCoordinates(
            Vertex<LayeredPartitionedLongWritable, V, E> vertex, int quantity,
            double slopeAngle, double slopeStart) throws IOException {
      double[][] placedVertices;
      double shortestEdge = vertex.getValue().getShortestEdge();
      if (isRadiusDynamic) {
        if (shortestEdge == Double.MAX_VALUE) {
          placedVertices = placeVerticesInGap(quantity, slopeAngle,
                  slopeStart, vertex.getValue().getCoordinates());
        } else {
          placedVertices = placeVerticesInGapWithShortestEdge(quantity,
                  slopeAngle, slopeStart, vertex.getValue().getCoordinates(),
                  shortestEdge);
        }
      } else {
        placedVertices = placeVerticesInGap(quantity, slopeAngle, slopeStart,
                vertex.getValue().getCoordinates());
      }
      return placedVertices;
    }

    protected void reconstructGraph(double[][] verticesToPlace,
            Vertex<LayeredPartitionedLongWritable, V, E> vertex)
            throws IOException {
      Iterator<LongWritable> oneDegreesIt = vertex.getValue()
              .getOneDegreeVertices();
      for (int i = 0; i < verticesToPlace.length; i++) {
        addSingleOneDegreeVertex(oneDegreesIt.next().get(),
                verticesToPlace[i], vertex);
      }
      if (oneDegreesIt.hasNext())
        throw new IOException("One Edges iterator was not completely explored");
    }

    protected void reconstructGraphKeepingIterator(double[][] verticesToPlace,
            Vertex<LayeredPartitionedLongWritable, V, E> vertex,
            Iterator<LongWritable> woundIterator) throws IOException {
      for (int i = 0; i < verticesToPlace.length; i++) {
        addSingleOneDegreeVertex(woundIterator.next().get(),
                verticesToPlace[i], vertex);
      }
    }

    @SuppressWarnings("unchecked")
    private void addSingleOneDegreeVertex(long idOfOneEdge,
      double[] coordinatesOfVertexToPlace,
            Vertex<LayeredPartitionedLongWritable, V, E> neighborVertex) {
      ArrayListEdges<LayeredPartitionedLongWritable, E> ale = new ArrayListEdges<LayeredPartitionedLongWritable, E>();
      ale.initialize(1);
      ale.add(((Edge<LayeredPartitionedLongWritable, E>) EdgeFactory.create(
              neighborVertex.getId(),
              WritableFactories.newInstance(getConf().getEdgeValueClass()))));
      V value = (V) WritableFactories.newInstance(getConf()
              .getVertexValueClass());
      value.setCoordinates(coordinatesOfVertexToPlace[0],
              coordinatesOfVertexToPlace[1]);
      value.setComponent(neighborVertex.getValue().getComponent());
      LayeredPartitionedLongWritable oE = new LayeredPartitionedLongWritable(
              neighborVertex.getId().getPartition(), idOfOneEdge);
      // oE.setId(idOfOneEdge);
      // oE.setPartition(neighborVertex.getId().getPartition());
      added++;
      try {
        addVertexRequest(oE, value, ale);
        addEdgeRequest(neighborVertex.getId(),
                ((Edge<LayeredPartitionedLongWritable, E>) EdgeFactory.create(
                        oE, WritableFactories
                                .newInstance(getConf().getEdgeValueClass()))));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    protected double[][] placeVerticesInCone(
            Vertex<LayeredPartitionedLongWritable, V, E> vertex, int size,
            Iterable<LayoutMessage> itl) throws IOException {
      double[] statusCopy = vertex.getValue().getCoordinates();
      double[] force = new double[] { 0.0f, 0.0f };

      // CoordinateWritable<Float> vValue = vertex.getValue();
      Iterator<LayoutMessage> cohords = itl.iterator();

      while (cohords.hasNext()) {

        LayoutMessage val = cohords.next();

        double[] foreignCoordinates = val.getValue();

        double distanceFromVertex = Toolbox.computeModule(statusCopy,
                new double[] { foreignCoordinates[0], foreignCoordinates[1] });

        double inverseSquareDist = 1 / Math.pow(distanceFromVertex,
                2);

        force[0] += (statusCopy[0] - foreignCoordinates[0])
                * inverseSquareDist;
        force[1] += (statusCopy[1] - foreignCoordinates[1])
                * inverseSquareDist;
      }

      double theta = Math.atan2(force[1], force[0]);
      double start = theta - aperture / 2;

      double[][] verticesToPlace = placeVerticesInGap(size, aperture, start,
              vertex.getValue().getCoordinates());
      return verticesToPlace;
    }

    @Override
    public void preSuperstep() {
      added = 0;
      k = ((DoubleWritable) getAggregatedValue(LayoutRoutine.k_agg)).get();
      radius = getConf().getDouble(LayoutRoutine.radiusString,
              LayoutRoutine.radiusDefault);
      isRadiusDynamic = getConf().getBoolean(
              LayoutRoutine.dynamicRadiusString, true);
    }
  }

  public static double angleBetween2Lines(Line2D line1, Line2D line2) {
    double angle1 = Math.atan2(line1.getY1() - line1.getY2(), line1.getX1()
            - line1.getX2());
    double angle2 = Math.atan2(line2.getY1() - line2.getY2(), line2.getX1()
            - line2.getX2());
    double result = angle1 - angle2;
    if (result < 0)
      result = Math.PI * 2 - Math.abs(result);
    return result;
  }

}
