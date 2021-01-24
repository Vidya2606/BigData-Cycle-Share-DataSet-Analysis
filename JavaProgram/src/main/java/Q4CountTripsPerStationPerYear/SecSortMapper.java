package Q4CountTripsPerStationPerYear;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecSortMapper extends Mapper <LongWritable, Text, YearStationPair, IntWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String year = null;
        String station = null;

        try {
            year = tokens[1].split("/")[2].substring(0, 4);
            station = tokens[7].substring(1, tokens[7].length() - 1);
        } catch (Exception e) {

        }

        if(year != null && station != null) {
            YearStationPair outkey = new YearStationPair(year, station);
            context.write(outkey, new IntWritable(1));
        }
    }
}
