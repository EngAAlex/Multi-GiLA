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

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.SpTreeEdgeValue;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.multi.coarseners.SolarMerger;
import unipg.gila.multi.io.SolarMergerTestOutputFormat.JavaSolarMergerTestVertexWriter;

public class SolarMergerTestWithSpanningTreeOutputFormat extends TextVertexOutputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> {

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
      float[] cohords = vertex.getValue().getCoordinates();
      //        if(vertex.getValue().isMoon())
      //          log.info("Moon " + vertex.getValue().getSun() + " of layer " + vertex.getId().getLayer() + " has " + vertex.getValue().getProxies().size() + " proxies");
      return new Text("[" + id.getId() + "," + id.getLayer() + "," + cohords[0] + "," + cohords[1] + "," + SolarMerger.AstralBody.toString(SolarMerger.AstralBody.buildBody(value.getDistanceFromSun())) + "," + ((LayeredPartitionedLongWritable)value.getSun()).getId() + ",[" + edgeBundlerWithSpanningTree(vertex.getEdges(), false) + "]," + "[" + edgeBundlerWithSpanningTree(vertex.getEdges(), true) + "]"  + "]");
    }
  }

  private String edgeBundlerWithSpanningTree(Iterable<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> edges, boolean includeSpanningTree){
    String result = "";
    boolean first = true;
    Iterator<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> it = edges.iterator();
    while(it.hasNext()){
      Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue> edge = it.next();
      if(edge.getValue().isSpanningTree() == includeSpanningTree){
        if(first)
          first = false;
        else
          result += ",";
        result += "[" + edge.getTargetVertexId().getId() + "," + edge.getTargetVertexId().getLayer() + "," + edge.getValue().getValue() + "]";
      }
    }

    return result;
  }	
}
