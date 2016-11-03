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
import java.io.IOException;
import java.util.LinkedList;

import unipg.gila.common.datastructures.LinkedListWritable;

/**
 * @author Alessio Arleo
 *
 */
public class ReferrersList extends LinkedListWritable<Referrer> {

  public ReferrersList() {
    internalState = new LinkedList<Referrer>();
  }

  public ReferrersList(LinkedListWritable<Referrer> toCopy) {
    this();
    if (toCopy != null && toCopy.size() > 0)
      addAll(toCopy);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * unipg.gila.common.datastructures.LinkedListWritable#specificRead(java.io
   * .DataInput)
   */
  @Override
  protected Referrer specificRead(DataInput in) throws IOException {
    Referrer current = new Referrer();
    current.readFields(in);
    return current;
  }

}
