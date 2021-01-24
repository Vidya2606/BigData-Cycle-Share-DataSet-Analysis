package Q5Top5BusiestStations;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top5Mapper extends Mapper <LongWritable, Text, IntWritable, StationCountWrittable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StationCountWrittable outvalue = new StationCountWrittable();
        if (value.toString().contains("starttime")) {
            return;
        }
        String[] tokens = value.toString().split(",");  // month, station count
        String[] stationCount = tokens[1].substring(1).split("\\s+");
        int month = Integer.parseInt(tokens[0]);
        outvalue.setStation(stationCount[0]);
        outvalue.setCount(Integer.parseInt(stationCount[stationCount.length - 1]));
        context.write(new IntWritable(month), outvalue);
    }
}
