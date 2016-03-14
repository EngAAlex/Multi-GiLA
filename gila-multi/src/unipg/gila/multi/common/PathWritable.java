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
package unipg.gila.multi.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class is a data structure used when   
 * 
 * @author Alessio Arleo
 *
 */
public class PathWritable implements Writable {

	private int positionInPath;
	private int pathLength;
	private LayeredPartitionedLongWritable referencedSun;
	
	public PathWritable() {
		referencedSun = new LayeredPartitionedLongWritable();
	}
	
	public PathWritable(int positionInPath, int pathLength, LayeredPartitionedLongWritable ref){
		this.positionInPath = positionInPath;
		this.pathLength = pathLength;
		referencedSun = ref;
	}
	
	public PathWritable copy(){
		return new PathWritable(positionInPath, pathLength, referencedSun.copy());
	}
	
	public int getPositionInpath(){
		return positionInPath;
	}
	
	public int getPathLength(){
		return pathLength;
	}
	
	public LayeredPartitionedLongWritable getReferencedSun(){
		return referencedSun;
	}

	public void readFields(DataInput in) throws IOException {
		positionInPath = in.readInt();
		pathLength = in.readInt();
		referencedSun.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(positionInPath);
		out.writeInt(pathLength);
		referencedSun.write(out);
	}

}
