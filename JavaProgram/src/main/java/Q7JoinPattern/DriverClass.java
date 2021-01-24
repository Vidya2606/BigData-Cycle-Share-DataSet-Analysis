package Q7JoinPattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DriverClass {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DriverClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TripMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), CombineTextInputFormat.class,WeatherMapper.class);

        job.setReducerClass(ReducerClass.class);

        job.getConfiguration().set("join.type",args[2]);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[3]), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
