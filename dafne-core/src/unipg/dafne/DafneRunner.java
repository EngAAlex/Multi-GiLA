package unipg.dafne;

import java.net.URI;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import unipg.dafne.partitioning.PrefixHashPartitionerFactory;

/**
 * This class is used to launch the job. It is a copy of the class "Giraph Runner" in package "org.apache.giraph" with the 
 * sole exception of the "setupGiraphConf" method, used to set the partitioner class.
 * 	
 * @author general
 *
 */
public class DafneRunner implements Tool{
	
	  static {
	    Configuration.addDefaultResource("giraph-site.xml");
	  }

	  /** Writable conf */
	  private Configuration conf;

	  @Override
	  public Configuration getConf() {
	    return conf;
	  }

	  @Override
	  public void setConf(Configuration conf) {
	    this.conf = conf;
	  }

	  @Override
	  /**
	   * Drives a job run configured for "Giraph on Hadoop MR cluster"
	   * @param args the command line arguments
	   * @return job run exit code
	   */
	  public int run(String[] args) throws Exception {
	    if (null == getConf()) { // for YARN profile
	      conf = new Configuration();
	    }
	    GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
	    setupGiraphConf(giraphConf);
	    CommandLine cmd = ConfigurationUtils.parseArgs(giraphConf, args);
	    if (null == cmd) {
	      return 0; // user requested help/info printout, don't run a job.
	    }
	    // set up job for various platforms
	    final String vertexClassName = args[0];
	    final String jobName = "Giraph: " + vertexClassName;
	    /*if[PURE_YARN]
	    GiraphYarnClient job = new GiraphYarnClient(giraphConf, jobName);
	    else[PURE_YARN]*/
	    GiraphJob job = new GiraphJob(giraphConf, jobName);
	    prepareHadoopMRJob(job, cmd);
	    /*end[PURE_YARN]*/

	    boolean verbose = !cmd.hasOption('q');
	    return job.run(verbose) ? 0 : -1;
	  }

	  private void setupGiraphConf(GiraphConfiguration giraphConf) {
		    giraphConf.setGraphPartitionerFactoryClass(PrefixHashPartitionerFactory.class);
	}

	/**
	   * Populate internal Hadoop Job (and Giraph IO Formats) with Hadoop-specific
	   * configuration/setup metadata, propagating exceptions to calling code.
	   * @param job the GiraphJob object to help populate Giraph IO Format data.
	   * @param cmd the CommandLine for parsing Hadoop MR-specific args.
	   */
	  private void prepareHadoopMRJob(final GiraphJob job, final CommandLine cmd)
	    throws Exception {
	    if (cmd.hasOption("vof")) {
	      if (cmd.hasOption("op")) {
	        FileOutputFormat.setOutputPath(job.getInternalJob(),
	          new Path(cmd.getOptionValue("op")));
	      }
	    }
	    if (cmd.hasOption("cf")) {
	      DistributedCache.addCacheFile(new URI(cmd.getOptionValue("cf")),
	          job.getConfiguration());
	    }
	  }

	  /**
	   * Execute GiraphRunner.
	   *
	   * @param args Typically command line arguments.
	   * @throws Exception Any exceptions thrown.
	   */
	  public static void main(String[] args) throws Exception {
	    System.exit(ToolRunner.run(new DafneRunner(), args));
	  }
}
