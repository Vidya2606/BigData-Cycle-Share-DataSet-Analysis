package Q7JoinPattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TripMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String[] datepart = tokens[1].substring(1).split(" ");

        outkey.set(datepart[0]);

        outvalue.set("A" + value.toString());

        context.write(outkey, outvalue);
    }
}
