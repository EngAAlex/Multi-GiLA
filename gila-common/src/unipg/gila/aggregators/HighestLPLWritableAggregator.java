/**
 * 
 */
package unipg.gila.aggregators;

import org.apache.giraph.aggregators.Aggregator;

import unipg.gila.common.multi.LayeredPartitionedLongWritable;

/**
 * @author Alessio Arleo
 *
 */
public class HighestLPLWritableAggregator implements
  Aggregator<LayeredPartitionedLongWritable> {

  LayeredPartitionedLongWritable currentMax;
  
  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#aggregate(org.apache.hadoop.io.Writable)
   */
  public void aggregate(LayeredPartitionedLongWritable value) {
    if(currentMax == null || value.getId() > currentMax.getId())
      currentMax = value.copy();     
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#createInitialValue()
   */
  public LayeredPartitionedLongWritable createInitialValue() {
    return new LayeredPartitionedLongWritable((short)0, Long.MIN_VALUE);
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#getAggregatedValue()
   */
  public LayeredPartitionedLongWritable getAggregatedValue() {
    return currentMax;
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#setAggregatedValue(org.apache.hadoop.io.Writable)
   */
  public void setAggregatedValue(LayeredPartitionedLongWritable value) {
    currentMax = value;
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#reset()
   */
  public void reset() {
    currentMax = createInitialValue();
  }

}
