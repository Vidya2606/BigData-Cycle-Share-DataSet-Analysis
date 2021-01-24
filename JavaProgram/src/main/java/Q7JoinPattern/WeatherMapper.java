package Q7JoinPattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper <LongWritable, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        outkey.set(tokens[0].substring(1, tokens[0].length()-1)); // date

        outvalue.set("B" + value.toString());

        context.write(outkey, outvalue);
    }
}
