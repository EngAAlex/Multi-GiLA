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
package unipg.gila.common.coordinatewritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactory;
import org.json.JSONArray;
import org.json.JSONException;

import unipg.gila.common.datastructures.LongWritableSet;

/**
 * This class models the core of the vertex value.
 * 
 * @author Alessio Arleo
 *
 */
public class MiniCoordinateWritable implements Writable, WritableFactory {

  /**
   * The vertex X coordinate.
   */
  protected double x;
  /**
   * The vertex Y coordinate.
   */
  protected double y;
  /**
   * A set containing the ids of its one degree neighbors.
   */
  protected LongWritableSet oneEdges;

  /**
   * The connected component index the vertex belongs to.
   */
  protected int component;

  public MiniCoordinateWritable() {
    x = 0.0f;
    y = 0.0f;
    component = -1;
    oneEdges = new LongWritableSet();
  }

  public MiniCoordinateWritable(double x, double y, int component) {
    this.x = x;
    this.y = y;
    this.component = component;
    oneEdges = new LongWritableSet();
  }

  public MiniCoordinateWritable(double x, double y, JSONArray oEs, int component)
          throws JSONException {
    this(x, y, component);
    oneEdges = new LongWritableSet();

    for (int i = 0; i < oEs.length(); i++)
      oneEdges.addElement(new LongWritable(oEs.getLong(i)));
  }

  public double[] getCoordinates() {
    return new double[] { x, y };
  }

  public void setCoordinates(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public int getOneDegreeVerticesQuantity() {
    if (oneEdges == null)
      return 0;
    return oneEdges.size();
  }

  public int getWeight() {
    return getOneDegreeVerticesQuantity() + 1;
  }

  /**
   * Returns an iterator on the one degree neighbors ids of the vertex.
   * 
   * @return Iterator on the one degree neighbors ids.
   */
  @SuppressWarnings("unchecked")
  public Iterator<LongWritable> getOneDegreeVertices() {
    return (Iterator<LongWritable>) oneEdges.iterator();
  }

  public void setComponent(int component) {
    this.component = component;
  }

  public int getComponent() {
    return component;
  }

  public void readFields(DataInput in) throws IOException {
    x = in.readDouble();
    y = in.readDouble();
    oneEdges.readFields(in);
    component = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    out.writeDouble(x);
    out.writeDouble(y);
    oneEdges.write(out);
    out.writeInt(component);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.WritableFactory#newInstance()
   */
  public Writable newInstance() {
    return new MiniCoordinateWritable();
  }

}