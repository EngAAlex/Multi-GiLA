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

/**
 * In this class presents the implementation for a set of SolarMessages capable of being (de)serialized.
 * 
 * @author Alessio Arleo
 *
 */
public class PathWritableSet extends SetWritable<PathWritable> {

	/**
	 * Parameter-less constructor.
	 * 
	 */
	public PathWritableSet() {
		internalState = new HashSet<PathWritable>();
	}
	
	/**
	 * This constructor will return a new LongWritableSet which is an exact copy of the given set.
	 * 
	 * @param toCopy the set to copy.
	 */
	public PathWritableSet(PathWritableSet toCopy){
		internalState = new HashSet<PathWritable>(toCopy.get());
	}
	
	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.SetWritable#specificRead(java.io.DataInput)
	 */
	@Override
	protected PathWritable specificRead(DataInput in) throws IOException {
		PathWritable pw = new PathWritable();
		pw.readFields(in);
		return pw;
	}

}
