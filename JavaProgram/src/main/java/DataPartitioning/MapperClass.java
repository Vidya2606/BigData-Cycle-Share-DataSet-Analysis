package DataPartitioning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper <Object, Text, IntWritable, Text> {

//    private IntWritable outkey = new IntWritable();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        int year = 0;

        try {
            year = Integer.parseInt(tokens[1].split("/")[2].substring(0, 4));
        } catch (Exception e) {
        }
//        outkey.set(year);
        context.write(new IntWritable(year), value);
    }
}
