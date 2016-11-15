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
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.SpTreeEdgeValue;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.layout.LayoutRoutine;

/**
 * Algorithm final output format. Each line represents a vertex and is formatted as follows:
 * 
 * [id, partition, connected component, x coordinate, y coordinate, [[neighbor id, weight]{,[neighbor id, weight]}]
 * 
 * The "partition" output is disabled by default and can be enabled using the option "layout.output.showPartitioning"; the "connected component" output is enabled by default and can be
 * disabled using the option "layout.output.showComponent".
 * 
 * @author Alessio Arleo
 *
 */
public class MultiScaleLayoutWithSpanningTreeOutputFormat extends
TextVertexOutputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> {

  @Override
  public TextVertexOutputFormat<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue>.TextVertexWriter createVertexWriter(
    TaskAttemptContext arg0) throws IOException,
    InterruptedException {
    return new LayerFilteredJSONWithPartitioningAndComponentVertexWriter();
  }

  protected class LayerFilteredJSONWithPartitioningAndComponentVertexWriter extends TextVertexWriterToEachLine {

    protected boolean showPartitioning;
    protected boolean showComponent;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
    InterruptedException {
      super.initialize(context);
      showPartitioning = getConf().getBoolean(LayoutRoutine.showPartitioningString, false);
      showComponent = getConf().getBoolean(LayoutRoutine.showComponentString, true);
    }

    @Override
    protected Text convertVertexToLine(
      Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex)
          throws IOException {
      if(vertex.getId().getLayer() != 0)
        return new Text("");
      float[] cohords = vertex.getValue().getCoordinates();
      String partition;
      String component;
      if(!showPartitioning)
        partition = "";
      else
        partition = vertex.getId().getPartition() + ",";

      if(!showComponent)
        component = "";
      else
        component = "," + vertex.getValue().getComponent();

      return new Text("[" + vertex.getId().getId() + "," + partition + cohords[0] + "," + cohords[1] + component + ",[" + edgeBundlerWithSpanningTree(vertex.getEdges(), false) + "]," + "[" + edgeBundlerWithSpanningTree(vertex.getEdges(), true) + "]"  + "]");
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
        result += "[" + edge.getTargetVertexId().getId() + "," + edge.getTargetVertexId().getPartition() + "]";
      }
    }
    return result;
  }


}
