/**
 * Copyright 2014 Grafos.ml
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
 */
package unipg.gila.partitioning;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PartitionedPositionedVertexValue implements WritableComparable {
	public static final String DELIMITER = "_";
	private Float[] coords;
	private short partition;
	private long id;

	public PartitionedPositionedVertexValue() {
	}

	public PartitionedPositionedVertexValue(String id) {
		String[] tokens = id.split(DELIMITER);
		this.partition = Short.parseShort(tokens[0]);
		this.id = Long.parseLong(tokens[1]);
		coords = new Float[]{0.0f, 0.0f};
	}
	
	public PartitionedPositionedVertexValue(String id, float x, float y) {
		String[] tokens = id.split(DELIMITER);
		this.partition = Short.parseShort(tokens[0]);
		this.id = Long.parseLong(tokens[1]);
		coords = new Float[]{x, y};
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		partition = in.readShort();
		id = in.readLong();
	}

	@Override
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
		PartitionedPositionedVertexValue other = (PartitionedPositionedVertexValue) o;
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

	public short getPartition() {
		return partition;
	}

	public long getId() {
		return id;
	}
	
	public Float[] getCoords(){
		return coords;
	}
	
	public void setCoords(Float[] coords){
		this.coords = coords;
	}

	@Override
	public int compareTo(Object o) {
		if (o == this) {
			return 0;
		}
		PartitionedPositionedVertexValue other = (PartitionedPositionedVertexValue) o;
		return this.id > other.id ? +1 : this.id < other.id ? -1 : 0;
	}
}
