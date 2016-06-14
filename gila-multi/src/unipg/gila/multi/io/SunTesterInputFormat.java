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

import org.apache.giraph.io.formats.TextVertexInputFormat;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.layout.LayoutRoutine;
import unipg.gila.utils.Toolbox;

import com.google.common.collect.Lists;


/**
 * @author Alessio Arleo
 *
 */

public class SunTesterInputFormat  extends
TextVertexInputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable>  {

	@Override
	public TextVertexInputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable>.TextVertexReader createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new SimpleJSONVertexReader();
	}

	protected class SimpleJSONVertexReader extends
	TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {

//		public SimpleJSONVertexReader(){
//			float nl = getConf().getFloat(LayoutRoutine.node_length , LayoutRoutine.defaultNodeValue);
//			float nw = getConf().getFloat(LayoutRoutine.node_width , LayoutRoutine.defaultNodeValue);
//			float ns = getConf().getFloat(LayoutRoutine.node_separation , LayoutRoutine.defaultNodeValue);
//			k = new Double(ns + Toolbox.computeModule(new float[]{nl, nw})).floatValue();
//		}
//		
//		float k;
		
		@Override
		protected JSONArray preprocessLine(Text line) throws JSONException {
			return new JSONArray(line.toString());
		}

		@Override
		protected LayeredPartitionedLongWritable getId(JSONArray jsonVertex) throws JSONException,
		IOException {
			return new LayeredPartitionedLongWritable((short) 0, jsonVertex.getLong(0));
		}

		@Override
		protected AstralBodyCoordinateWritable getValue(JSONArray jsonVertex) throws
		JSONException, IOException {
			return new AstralBodyCoordinateWritable((float)jsonVertex.getDouble(1), (float)jsonVertex.getDouble(2), 0);
		}

		protected Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> handleException(Text line, JSONArray jsonVertex,
				Exception e) {
			return null;
		}

		protected Iterable<Edge<LayeredPartitionedLongWritable, IntWritable>> getEdges(JSONArray jsonVertex) throws JSONException, IOException {
			JSONArray jsonEdgeArray = jsonVertex.getJSONArray(3);
			List<Edge<LayeredPartitionedLongWritable, IntWritable>> edges = Lists.newArrayList();
			for (int i = 0; i < jsonEdgeArray.length(); ++i) {
//				JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
				edges.add(EdgeFactory.create(new LayeredPartitionedLongWritable((short) 0, jsonEdgeArray.getLong(i)),
						new IntWritable(1)));
			}
			return edges;
		}

	}
}



