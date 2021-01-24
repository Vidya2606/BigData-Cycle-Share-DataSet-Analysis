package Q8Top5BusyRoutes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecSortMapper extends Mapper <LongWritable, Text, FromToStationPair, IntWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        String fromstn = null;
        String tostn = null;

        try {
            fromstn = tokens[7].substring(1, tokens[7].length() - 1);
            tostn = tokens[8].substring(1, tokens[7].length()-1);
        } catch (Exception e) {

        }

        if(fromstn != null && tostn != null) {
            FromToStationPair outkey = new FromToStationPair(fromstn, tostn);
            if (fromstn.compareTo(tostn) > 0) {
                outkey.setToStationId(fromstn);
                outkey.setFromStationId(tostn);
            }
            context.write(outkey, new IntWritable(1));
        }
    }
}
