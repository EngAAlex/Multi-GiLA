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
package unipg.gila.common.multi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Alessio Arleo
 *
 */
public class Referrer implements Writable {

  LayeredPartitionedLongWritable eventGenerator;
  int distanceAccumulator = 0;

  /**
   * 
   */
  public Referrer() {
    eventGenerator = new LayeredPartitionedLongWritable();
  }

  /**
   * 
   */
  public Referrer(LayeredPartitionedLongWritable eventG, int distance) {
    eventGenerator = eventG;
    distanceAccumulator = distance;
  }

  /**
   * @return the eventGenerator
   */
  public LayeredPartitionedLongWritable getEventGenerator() {
    return eventGenerator;
  }

  /**
   * @return the distanceAccumulator
   */
  public int getDistanceAccumulator() {
    return distanceAccumulator;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    eventGenerator.write(out);
    out.writeInt(distanceAccumulator);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    eventGenerator.readFields(in);
    distanceAccumulator = in.readInt();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "Generator " + eventGenerator + " distance " + distanceAccumulator;
  }
}
