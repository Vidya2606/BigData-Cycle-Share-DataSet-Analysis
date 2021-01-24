package Q5Top5BusiestStations;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper <LongWritable, Text, MonthStationPair, IntWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        String  month = null;
        String station = null;

        try {
            month = tokens[1].split("/")[0].substring(1);
            station = tokens[7].substring(1, tokens[7].length() - 1);
        } catch (Exception e) {

        }

        if(month != null  && station != null) {
            MonthStationPair outkey = new MonthStationPair(month, station);
            context.write(outkey, new IntWritable(1));
        }
    }
}
