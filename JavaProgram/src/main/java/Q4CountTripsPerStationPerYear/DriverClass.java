package Q4CountTripsPerStationPerYear;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DriverClass {
    public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tripsPerYearPerStation");
        job.setJarByClass(DriverClass.class);

        job.setGroupingComparatorClass(SecSortGroupComparator.class);
        job.setSortComparatorClass(SecSortComparator.class);
        job.setPartitionerClass(KeyPartitioner.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        Path outDir = new Path(args[1]);
        TextOutputFormat.setOutputPath(job, outDir);

        job.setMapperClass(SecSortMapper.class);
        job.setReducerClass(SecSortReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(YearStationPair.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outDir)) {
            fs.delete(outDir, true);
        }
        job.waitForCompletion(true);
    }
}
