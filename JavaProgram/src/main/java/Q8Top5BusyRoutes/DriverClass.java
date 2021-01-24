package Q8Top5BusyRoutes;

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
        Job job = Job.getInstance(conf, "fromTostationPairCount");
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

        job.setOutputKeyClass(FromToStationPair.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outDir)) {
            fs.delete(outDir, true);
        }
        boolean result = job.waitForCompletion(true);

        if(result)
        {
            Job job1 = Job.getInstance();
            job1.setJarByClass(DriverClass.class);

            job1.setMapperClass(Top10Mapper.class);
            job1.setReducerClass(Top10Reducer.class);

            job1.setMapOutputKeyClass(FromToStationPair.class);
            job1.setMapOutputValueClass(IntWritable.class);

            job1.setOutputKeyClass(FromToStationPair.class);
            job1.setOutputValueClass(IntWritable.class);

            // input to this is the output of the first mapper
            TextInputFormat.addInputPath(job1, new Path(args[1]));
            // final output will be stored in the 3rd argument
            Path finalOutputDirectory = new Path(args[2]);

            TextOutputFormat.setOutputPath(job1, finalOutputDirectory);

            FileSystem fs2 = FileSystem.get(job1.getConfiguration());
            if(fs2.exists(finalOutputDirectory))
            {
                fs2.delete(finalOutputDirectory, true);
            }
            job1.waitForCompletion(true);
        }
    }
}
