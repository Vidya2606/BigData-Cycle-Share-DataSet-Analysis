package Q9CountMemershipByGender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DriverClass {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DriverClass.class);

        job.setMapperClass(MapperClass.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]), true);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if(code == 0) {
            for (Counter counter: job.getCounters().getGroup(MapperClass.GENDER)) {
                System.out.println(counter.getDisplayName()+ "\t" + counter.getValue());
            }
        }
        // we don't want to save the output in the directory, so we are deleting it
        fs.get(conf).delete(new Path(args[1]), true);

        System.exit(code);
    }
}
