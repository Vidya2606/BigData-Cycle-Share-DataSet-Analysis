package DataPartitioning;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<IntWritable, Text> implements Configurable {

    private static final String YEAR_MIN = "year.min";

    private Configuration conf = null;
    private int year = 0;

    public int getPartition(IntWritable key, Text value, int numPartitions) {
        return key.get() - year;
    }
    public Configuration getConf() {
        return conf;
    }
    public void setConf(Configuration conf) {
        this.conf = conf;
        year = conf.getInt(YEAR_MIN, 0);
    }

    public static void setYearMin(Job job, int year) {
        job.getConfiguration().setInt(YEAR_MIN, year);
    }




}
