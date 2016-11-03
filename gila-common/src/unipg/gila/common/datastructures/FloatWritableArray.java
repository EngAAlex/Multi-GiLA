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
package unipg.gila.common.datastructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A class representing an array of floats implementing the Writable interface.
 * 
 * @author Alessio Arleo
 *
 */
public class FloatWritableArray implements Writable {

  private float[] internalState;

  public FloatWritableArray() {
  }

  public FloatWritableArray(float[] in) {
    internalState = new float[in.length];
    for (int i = 0; i < in.length; i++)
      internalState[i] = in[i];
  }

  public float[] get() {
    return internalState;
  }

  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    internalState = new float[length];
    for (int i = 0; i < length; i++)
      internalState[i] = in.readFloat();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(internalState.length);
    for (int i = 0; i < internalState.length; i++)
      out.writeFloat(internalState[i]);
  }

  @Override
  public String toString() {
    String result = "";
    for (float f : internalState)
      result += result.equals("") ? f : ", " + f;
    return result;
  }

}
