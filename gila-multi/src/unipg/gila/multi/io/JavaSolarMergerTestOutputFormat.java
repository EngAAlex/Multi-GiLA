/**
 * 
 */
package unipg.gila.multi.io;

import org.apache.giraph.io.formats.TextVertexOutputFormat;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import unipg.gila.multi.coarseners.SolarMerger;
import unipg.gila.multi.common.AstralBodyCoordinateWritable;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;

public class JavaSolarMergerTestOutputFormat extends TextVertexOutputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> {

	@Override
	public TextVertexOutputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable>.TextVertexWriter createVertexWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
			return new JavaSolarMergerTestVertexWriter();
		}

		protected class JavaSolarMergerTestVertexWriter extends TextVertexWriterToEachLine {

			Logger log = Logger.getLogger(JavaSolarMergerTestVertexWriter.class);
			
			@Override
			protected Text convertVertexToLine(
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, IntWritable> vertex)
					throws IOException {
				LayeredPartitionedLongWritable id = vertex.getId();
				AstralBodyCoordinateWritable value = vertex.getValue();
				float[] cohords = vertex.getValue().getCoordinates();
//				if(vertex.getValue().isMoon())
//					log.info("Moon " + vertex.getValue().getSun() + " of layer " + vertex.getId().getLayer() + " has " + vertex.getValue().getProxies().size() + " proxies");
				return new Text("[" + id.getId() + "," + id.getLayer() + "," + cohords[0] + "," + cohords[1] + "," + SolarMerger.AstralBody.toString(SolarMerger.AstralBody.buildBody(value.getDistanceFromSun())) + "," + ((LayeredPartitionedLongWritable)value.getSun()).getId() + ",[" + edgeBundler(vertex.getEdges()) + "]]");
			}
		}

		private String edgeBundler(Iterable<Edge<LayeredPartitionedLongWritable, IntWritable>> edges){
			String result = "";
			Iterator<Edge<LayeredPartitionedLongWritable, IntWritable>> it = edges.iterator();
			while(it.hasNext()){
				Edge<LayeredPartitionedLongWritable, IntWritable> edge = it.next();
				result += "[" + edge.getTargetVertexId().getId() + "," + edge.getTargetVertexId().getLayer() + "," + edge.getValue().get() + "]";
				if(it.hasNext())
					result += ",";
			}
			return result;
		}
	
}
