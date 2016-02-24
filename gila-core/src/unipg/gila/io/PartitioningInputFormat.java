package unipg.gila.io;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.EdgeValue;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.PartitioningVertexValue;

/**
 * This class is the default input method for the partitioning algorithm. It accepts text files representing graphs in which each line represents
 * a vertex and is structured as follows:
 * 
 * [id, x coordinate, y coordinate, [[neighbor id] {, [neighbor id]}*]
 * 
 * @author Alessio Arleo
 *
 */
public class PartitioningInputFormat extends
TextVertexInputFormat<LongWritable, PartitioningVertexValue, EdgeValue> {
	
	@Override
	public TextVertexInputFormat<LongWritable, PartitioningVertexValue, EdgeValue>.TextVertexReader createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new PartitioningVertexReader();
	}

	protected class PartitioningVertexReader extends
	TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray, JSONException> {

		@Override
		protected JSONArray preprocessLine(Text line) throws JSONException {
			return new JSONArray(line.toString());
		}

		@Override
		protected LongWritable getId(JSONArray jsonVertex) throws JSONException,
		IOException {
			return new LongWritable(jsonVertex.getLong(0));
		}

		@Override
		protected PartitioningVertexValue getValue(JSONArray jsonVertex) throws
		JSONException, IOException {
			return new PartitioningVertexValue(new float[]{new Double(jsonVertex.getDouble(1)).floatValue(), new Double(jsonVertex.getDouble(2)).floatValue()});
		}

		protected Vertex<PartitionedLongWritable, CoordinateWritable, FloatWritable> handleException(Text line, JSONArray jsonVertex,
				Exception e) {
			return null;
		}

		protected Iterable<Edge<LongWritable, EdgeValue>> getEdges(JSONArray jsonVertex) throws JSONException, IOException {
			JSONArray jsonEdgeArray = jsonVertex.getJSONArray(3);
			List<Edge<LongWritable, EdgeValue>> edges =	Lists.newArrayList();
			int i;
			for (i = 0; i < jsonEdgeArray.length(); ++i) {
				long neighborId = jsonEdgeArray.getLong(i);
				edges.add(EdgeFactory.create(new LongWritable(neighborId),
						new EdgeValue()));
			}
			return edges;
		}

	}

}
