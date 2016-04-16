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
import java.util.HashSet;

import unipg.gila.common.datastructures.SetWritable;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;

/**
 * @author Alessio Arleo
 *
 */
public class LayeredPartitionedLongWritableSet extends SetWritable<LayeredPartitionedLongWritable> {

	/**
	 * Parameter-less constructor.
	 * 
	 */
	public LayeredPartitionedLongWritableSet() {
		internalState = new HashSet<LayeredPartitionedLongWritable>();
	}
	
	/**
	 * This constructor will return a new LongWritableSet which is an exact copy of the given set.
	 * 
	 * @param toCopy the set to copy.
	 */
	public LayeredPartitionedLongWritableSet(LayeredPartitionedLongWritableSet toCopy){
		internalState = new HashSet<LayeredPartitionedLongWritable>(toCopy.get());
	}
	
	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.SetWritable#specificRead(java.io.DataInput)
	 */
	@Override
	protected LayeredPartitionedLongWritable specificRead(DataInput in)
			throws IOException {
		LayeredPartitionedLongWritable lpw = new LayeredPartitionedLongWritable();
		lpw.readFields(in);
		return lpw;
	}

}
