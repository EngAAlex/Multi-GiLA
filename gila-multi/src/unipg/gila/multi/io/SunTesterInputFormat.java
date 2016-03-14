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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import unipg.gila.multi.common.AstralBodyCoordinateWritable;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;

import com.google.common.collect.Lists;


/**
 * @author Alessio Arleo
 *
 */

public class SunTesterInputFormat  extends
TextVertexInputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable>  {

	@Override
	public TextVertexInputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable>.TextVertexReader createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new SimpleJSONVertexReader();
	}

	protected class SimpleJSONVertexReader extends
	TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {

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
			return new AstralBodyCoordinateWritable(new Double(jsonVertex.getDouble(1)).floatValue(), new Double(jsonVertex.getDouble(2)).floatValue(), 0);
		}

		protected Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> handleException(Text line, JSONArray jsonVertex,
				Exception e) {
			return null;
		}

		protected Iterable<Edge<LayeredPartitionedLongWritable, FloatWritable>> getEdges(JSONArray jsonVertex) throws JSONException, IOException {
			JSONArray jsonEdgeArray = jsonVertex.getJSONArray(3);
			List<Edge<LayeredPartitionedLongWritable, FloatWritable>> edges = Lists.newArrayList();
			for (int i = 0; i < jsonEdgeArray.length(); ++i) {
//				JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
				edges.add(EdgeFactory.create(new LayeredPartitionedLongWritable((short) 0, jsonEdgeArray.getLong(i)),
						new FloatWritable(1.0f)));
			}
			return edges;
		}

	}
}



