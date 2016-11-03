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
package unipg.gila.common.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import unipg.gila.common.datastructures.EdgeValue;
import unipg.gila.common.datastructures.PartitioningVertexValue;
import unipg.gila.common.partitioning.Spinner;

/**
 * This class outputs the graph partitioning phase result into a text format in
 * which each line represents a vertex and is structured as follows:
 * 
 * [id, vertex connected component, xcoord, ycoord, n. of pruned one degree
 * vertices associated with the vertex, [[neighbor id] {, [neighbor id]}*]]
 * 
 * It fits LayoutInputFormat. Vertex coordinates can be randomized using the
 * "spinner.doRandomizeCoordinates" option.
 * 
 * @author Alessio Arleo
 *
 */
public class PartitioningOutputFormat
        extends
        TextVertexOutputFormat<LongWritable, PartitioningVertexValue, EdgeValue> {

  @Override
  public TextVertexOutputFormat<LongWritable, PartitioningVertexValue, EdgeValue>.TextVertexWriter createVertexWriter(
          TaskAttemptContext arg0) throws IOException, InterruptedException {
    return new SpinnerUtilJSONPartitionedWoELongArrayFloatVertexWriter();
  }

  public class SpinnerUtilJSONPartitionedWoELongArrayFloatVertexWriter extends
          TextVertexWriterToEachLine {

    protected Logger log = Logger
            .getLogger(SpinnerUtilJSONPartitionedWoELongArrayFloatVertexWriter.class);

    protected boolean doRandomize;
    protected boolean showComponent;

    protected float bBoxX;
    protected float bBoxY;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
            InterruptedException {
      super.initialize(context);
      doRandomize = getConf().getBoolean(Spinner.doRandomizeString, true);
      if (doRandomize) {
        bBoxX = getConf().getFloat(Spinner.bBoxStringX, 1200.0f);
        bBoxY = getConf().getFloat(Spinner.bBoxStringY, bBoxX);
      }

      showComponent = getConf().getBoolean(Spinner.showComponent, true);
    }

    @Override
    protected Text convertVertexToLine(
            Vertex<LongWritable, PartitioningVertexValue, EdgeValue> vertex)
            throws IOException {
      PartitioningVertexValue vValue = vertex.getValue();
      float finalX;
      float finalY;
      String component = "";
      if (doRandomize) {
        finalX = (float) (Math.random() * bBoxX);
        finalY = (float) (Math.random() * bBoxY);
      } else {
        finalX = vertex.getValue().getCoords()[0];
        finalY = vertex.getValue().getCoords()[1];
      }
      if (showComponent)
        component += vValue.getComponent() + ",";

      String oneEdges = "[";

      if (vValue.getOneEdgesNo() > 0) {
        Iterator<LongWritable> it = vValue.getOneEdges();

        while (it.hasNext()) {
          oneEdges += it.next().get();
          if (it.hasNext())
            oneEdges += ",";
        }
      }

      oneEdges += "]";

      return new Text("[" + vertex.getId() + "," + component
              + vValue.getCurrentPartition() + "," + finalX + "," + finalY
              + "," + oneEdges + ",[" + edgeBundler(vertex.getEdges()) + "]]");
    }

  }

  private static String edgeBundler(
          Iterable<Edge<LongWritable, EdgeValue>> edges) {
    String result = "";
    Iterator<Edge<LongWritable, EdgeValue>> it = edges.iterator();
    while (it.hasNext()) {
      Edge<LongWritable, EdgeValue> edge = it.next();
      result += "[" + edge.getTargetVertexId() + ","
              + edge.getValue().getPartition() + "]";
      if (it.hasNext())
        result += ",";
    }
    return result;
  }

}
