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
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * A class representing a Linked List implementing the Writable interface.
 * 
 * @author Alessio Arleo
 *
 */
public abstract class LinkedListWritable<T extends Writable> implements
        Writable {

  protected LinkedList<T> internalState;

  // Logger log = Logger.getLogger(LinkedListWritable.class);

  public void addAll(LinkedListWritable<T> toAdd) {
    if (toAdd == null)
      return;
    Iterator<T> it = toAdd.iterator();
    while (it.hasNext())
      internalState.add(it.next());
  }

  public void enqueue(T toEnqueue) {
    internalState.addFirst(toEnqueue);
  }

  public T dequeue() {
    if (!isEmpty())
      return internalState.pop();
    return null;
  }

  public int size() {
    return internalState.size();
  }

  public boolean isEmpty() {
    return internalState.isEmpty();
  }

  public Iterator<T> iterator() {
    return internalState.iterator();
  }

  public Writable[] flush() {
    Writable[] result = internalState.toArray(new Writable[0]);
    internalState.clear();
    return result;
  }

  public void readFields(DataInput in) throws IOException {
    internalState.clear();

    int numFields = in.readInt();

    if (numFields == 0)
      return;
    for (int i = 0; i < numFields; i++)
      internalState.add(specificRead(in));
    // String className = in.readUTF();
    // T obj;
    // try {
    // Class<T> c = (Class<T>) Class.forName(className);
    // for (int i = 0; i < numFields; i++) {
    // obj = (T) c.newInstance();
    // obj.readFields(in);
    // internalState.add(obj);
    // }
    //
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
  }

  public void write(DataOutput out) throws IOException {
    int size = internalState.size();
    out.writeInt(size);
    if (size == 0)
      return;
    Iterator<T> it = internalState.iterator();
    while (it.hasNext())
      it.next().write(out);
    // Writable obj = internalState.get(0);
    //
    // out.writeUTF(obj.getClass().getCanonicalName());
    //
    // for (int i = 0; i < size; i++) {
    // obj = internalState.get(i);
    // if (obj == null) {
    // throw new IOException("Cannot serialize null fields!");
    // }
    // obj.write(out);
    // }
  }

  /**
   * @param in
   * @throws IOException
   */
  protected abstract T specificRead(DataInput in) throws IOException;

  public String toString() {
    if (internalState.isEmpty())
      return "EMPTY\n";
    Iterator<T> it = internalState.iterator();
    String tmp = "";
    while (it.hasNext())
      tmp += it.next().toString() + "\n";
    return tmp;
  }

}
