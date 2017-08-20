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
package unipg.gila.multi.io;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.SpTreeEdgeValue;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.common.multi.PathWritable;

import com.google.common.collect.Lists;

/**
 * This class is used to load data for the layout computation. It is a JSON format formed as follows:
 * 
 * [id, partition, connected component, x coordinate, y coordinate, n. of one degree vertices neighbors,[ [neighbor vertex Id, neighbor vertex partition] {,[neighbor vertex Id, neighbor vertex partition]}* ]]
 * 
 * It fits SpinnerVertexOutputFormat.
 * 
 * @author Alessio Arleo
 *
 */
public class CompleteSolarInfoInputFormat extends
TextVertexInputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> {

	@Override
	public TextVertexInputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue>.TextVertexReader createVertexReader(
			InputSplit in, TaskAttemptContext out) throws IOException {
		return new JSONPartitionedLongArrayFloatVertexReader();
	}

	protected class JSONPartitionedLongArrayFloatVertexReader extends
	TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {

		@Override
		protected JSONArray preprocessLine(Text line) throws JSONException {
			return new JSONArray(line.toString());
		}

		@Override
		protected LayeredPartitionedLongWritable getId(JSONArray jsonVertex) throws JSONException,
		IOException {
			return new LayeredPartitionedLongWritable((short)jsonVertex.getInt(0), jsonVertex.getLong(1), jsonVertex.getInt(2));
		}

		@Override
		protected AstralBodyCoordinateWritable getValue(JSONArray jsonVertex) throws
		JSONException, IOException {
			AstralBodyCoordinateWritable abcw = new AstralBodyCoordinateWritable(jsonVertex.getInt(3), 
					jsonVertex.getDouble(5), jsonVertex.getDouble(6), 
					jsonVertex.getJSONArray(20), jsonVertex.getInt(7));
			switch(jsonVertex.getInt(8)){
			case 1: abcw.setAsPlanet(jsonVertex.getInt(9)); 
			abcw.setSun(
					new LayeredPartitionedLongWritable(
							getShortFromInt(jsonVertex.getInt(10)), 
							jsonVertex.getLong(11), 
							jsonVertex.getInt(12)));
			break;
			case 2: abcw.setAsMoon(jsonVertex.getInt(9)); 
			abcw.setSun(
					new LayeredPartitionedLongWritable(
							getShortFromInt(jsonVertex.getInt(10)), 
							jsonVertex.getLong(11), 
							jsonVertex.getInt(12)),
					new LayeredPartitionedLongWritable(
							getShortFromInt(jsonVertex.getInt(13)), 
							jsonVertex.getLong(14), 
							jsonVertex.getInt(15)));
			JSONArray proxies = jsonVertex.getJSONArray(16);
			for(int i=0; i<proxies.length(); i++){
				JSONArray current = proxies.getJSONArray(i);
				abcw.addToProxies(
						new LayeredPartitionedLongWritable(
								getShortFromInt(current.getInt(0)), 
								current.getLong(1), 
								current.getInt(2))
						);
			}
			break;
			default: abcw.setAsSun(); 
			JSONArray planets = jsonVertex.getJSONArray(17);
			JSONArray moons = jsonVertex.getJSONArray(18);
			JSONArray neighs = jsonVertex.getJSONArray(19);
			int i;
			JSONArray paths = null;
			for(i=0; i<planets.length(); i++){
				JSONArray current = planets.getJSONArray(i);
				LayeredPartitionedLongWritable planet = 
						new LayeredPartitionedLongWritable(
								getShortFromInt(current.getInt(0)), 
								current.getLong(1), 
								current.getInt(2));
				abcw.addPlanet(planet);
				paths = current.getJSONArray(3);
				fillPathWritableSets(planet, abcw, paths);
			}

			for(i=0; i<moons.length(); i++){
				JSONArray current = moons.getJSONArray(i);
				LayeredPartitionedLongWritable moon = 
						new LayeredPartitionedLongWritable(
								getShortFromInt(current.getInt(0)), 
								current.getLong(1), 
								current.getInt(2));				
				abcw.addMoon(moon);
				paths = current.getJSONArray(3);
				fillPathWritableSets(moon, abcw, paths);				
			}

			for(i=0; i<neighs.length(); i++){
				JSONArray current = neighs.getJSONArray(i);
				abcw.addNeighbourSystem(
						new LayeredPartitionedLongWritable(
								getShortFromInt(current.getInt(0)), 
								current.getLong(1), 
								current.getInt(2)),
						current.getInt(3)					
				);
			}
			break;
			}
			return abcw;
		}

		/**
		 * @param abcw
		 * @param paths
		 */
		private void fillPathWritableSets(LayeredPartitionedLongWritable element, AstralBodyCoordinateWritable abcw, JSONArray paths) {
			for(int t=0; t<paths.length(); t++){
				JSONArray currentPath = paths.getJSONArray(t);
				PathWritable pw = 
						new PathWritable(
						currentPath.getInt(3),
						new LayeredPartitionedLongWritable(
								getShortFromInt(currentPath.getInt(0)), 
								currentPath.getLong(1), 
								currentPath.getInt(2))
						);
				abcw.addPathWritableToSet(element, pw);
			}			
		}

		protected Vertex<PartitionedLongWritable, CoordinateWritable, FloatWritable> handleException(Text line, JSONArray jsonVertex,
				Exception e) {
			return null;
		}

		protected Iterable<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> getEdges(JSONArray jsonVertex) throws JSONException, IOException {
			JSONArray jsonEdgeArray = jsonVertex.getJSONArray(21);
			List<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> edges =	Lists.newArrayList();
			for (int i = 0; i < jsonEdgeArray.length(); i++) {
				JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
				edges.add(EdgeFactory.create(new LayeredPartitionedLongWritable(
						getShortFromInt(jsonEdge.getInt(0)), 
						jsonEdge.getLong(1), 
						jsonEdge.getInt(2)),
						new SpTreeEdgeValue(jsonEdge.getInt(3))));
			}
			return edges;
		}
		
		private short getShortFromInt(int i){
			return new Integer(i).shortValue();
		}

	}

}
