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
package unipg.gila.multi.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.SpTreeEdgeValue;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.common.multi.LayeredPartitionedLongWritableSet;
import unipg.gila.common.multi.PathWritable;
import unipg.gila.common.multi.PathWritableSet;
import unipg.gila.multi.coarseners.SolarMerger;

public class CompleteSolarInfoOutputFormat extends TextVertexOutputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> {

	@Override
	public TextVertexOutputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue>.TextVertexWriter createVertexWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		return new JavaSolarMergerTestVertexWriter();
	}

	protected class JavaSolarMergerTestVertexWriter extends TextVertexWriterToEachLine {

		Logger log = Logger.getLogger(JavaSolarMergerTestVertexWriter.class);

		@Override
		protected Text convertVertexToLine(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex)
						throws IOException {
			LayeredPartitionedLongWritable id = vertex.getId();
			AstralBodyCoordinateWritable value = vertex.getValue();
			double[] cohords = vertex.getValue().getCoordinates();
			String planets = "";
			String moons = "";
			String neighbors = "";
			String proxies = "";
			if(value.isSun()){
				planets = celestialBodyBundler(value.getPlanetsIterator());
				moons = celestialBodyBundler(value.getMoonsIterator());
				neighbors = neighborsBundler(value.neighbourSystemsIterator());
			}else if(!value.isAsteroid()){
				proxies = proxyBundler(value.getProxies());
			}
			return new Text("[" + id.toString() + "," +
					value.getLowerLevelWeight() + "," + value.astralWeight() + 
					"," + cohords[0] + "," + cohords[1] + "," + value.getComponent() +
					"," + value.getDistanceFromSun() + 
					"," + value.getWeightFromSun() + 
					"," + (value.isSun() ? "-1,-1,-1" : ((LayeredPartitionedLongWritable)value.getSun()).toString()) + 
					"," + (value.isAsteroid() || value.isSun() ? "-1,-1,-1" : value.getProxy().toString()) + 
					",[" + proxies + "]" +
					",[" + planets + "]" +
					",[" + moons + "]" +
					",["+ neighbors +"]" +
					",["+ oneEdgeBundler(value) + "],[" + edgeBundler(vertex.getEdges()) + "]]");
		}

	}

	private String edgeBundler(Iterable<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> edges){
		String result = "";
		Iterator<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> it = edges.iterator();
		while(it.hasNext()){
			Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue> edge = it.next();
			result += "[" + edge.getTargetVertexId().toString() + "," + edge.getValue().getValue() + "]";
			if(it.hasNext())
				result += ",";
		}
		return result;
	}

	private String oneEdgeBundler(AstralBodyCoordinateWritable value){
		String oneEdges = "";
		if (value.getOneDegreeVerticesQuantity() > 0) {
			Iterator<LongWritable> it = value.getOneDegreeVertices();
			while (it.hasNext()) {
				oneEdges += it.next().get();
				if (it.hasNext())
					oneEdges += ",";
			}
		}
		return oneEdges;
	}

	private String celestialBodyBundler(Iterator<Entry<Writable, Writable>> it){
		String result = "";
		if(it == null)
			return result;
		while(it.hasNext()){
			Entry<Writable, Writable> next = it.next();				
			LayeredPartitionedLongWritable id = (LayeredPartitionedLongWritable) next.getKey();
			PathWritableSet pSet = (PathWritableSet) next.getValue();
			result += "[";
			result += id.toString() + ",[";
			Iterator<? extends Writable> pIt = pSet.iterator();
			while(pIt.hasNext()){
				PathWritable pNext = (PathWritable) pIt.next();
				result += "[";
				result += pNext.getReferencedSun().toString() + "," + pNext.getPositionInpath();
				result += "]";
				if(pIt.hasNext())
					result += ",";
			}
			result += "]]";
			if(it.hasNext())
				result += ",";
		}
		return result;			
	}

	/**
	 * @param neighbourSystemsIterator
	 * @return
	 */
	private String neighborsBundler(Iterator<Entry<Writable, Writable>> neighbourSystemsIterator) {
		String result = "";
		while(neighbourSystemsIterator.hasNext()){
			Entry<Writable, Writable> next = neighbourSystemsIterator.next();
			LayeredPartitionedLongWritable id = (LayeredPartitionedLongWritable) next.getKey();
			IntWritable v = (IntWritable) next.getValue();
			result += "[";
			result += id.toString() + "," + v.toString();
			result += "]";
			if(neighbourSystemsIterator.hasNext())
				result += ",";
		}

		return result;
	}

	/**
	 * @param value
	 * @return
	 */
	private String proxyBundler(LayeredPartitionedLongWritableSet value) {
		String result = "";
		if(value == null)
			return result;
		Iterator<? extends Writable> it = value.iterator();
		while(it.hasNext()){
			result += "[";
			LayeredPartitionedLongWritable current = (LayeredPartitionedLongWritable) it.next();
			result += current.toString();
			result += "]";
			if(it.hasNext())
				result += ",";
		}
		return result;
	}



}
