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

import org.apache.hadoop.io.WritableComparable;

/**
 * This class identifies a vertex by its id and partition.
 * 
 * @author claudio
 *
 */
public class PartitionedLongWritable implements WritableComparable<Object>{
		
		public static final String DELIMITER = "_";
		protected short partition = -1;
		protected long id = -1;

		public PartitionedLongWritable() {

		}

		public PartitionedLongWritable(String id) {
			String[] tokens = id.split(DELIMITER);
			this.partition = Short.parseShort(tokens[0]);
			this.id = Long.parseLong(tokens[1]);
		}
		
		public PartitionedLongWritable(short partition, long id){
			this.partition = partition;
			this.id = id;
		}
		
		public PartitionedLongWritable(PartitionedLongWritable idToCopy) {
			this.partition = idToCopy.getPartition();
			this.id = idToCopy.getId();	
		}

		public PartitionedLongWritable copy(){
			return new PartitionedLongWritable(partition, id);
		}

		public void readFields(DataInput in) throws IOException{
				partition = in.readShort();
				id = in.readLong();			
		}

		public void write(DataOutput out) throws IOException {
			out.writeShort(partition);
			out.writeLong(id);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			PartitionedLongWritable other = (PartitionedLongWritable) o;
			if (this.partition == other.partition && this.id == other.id) {
				return true;
			}
			return false;
		}

		@Override
		public String toString() {
			return partition + DELIMITER + id;
		}

		@Override
		public int hashCode() {
			return (int) id;
		}

		public Short getPartition() {
			return partition;
		}

		public Long getId() {
			return id;
		}

		public int compareTo(Object o) {
			if (o == this) {
				return 0;
			}
			PartitionedLongWritable other = (PartitionedLongWritable) o;
			return this.id > other.id ? +1 : this.id < other.id ? -1 : 0;
		}
		
			
	}

