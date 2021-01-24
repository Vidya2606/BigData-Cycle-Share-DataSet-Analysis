package Q3MinMaxAvgTripDuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper <LongWritable, Text, Text, TripWritable> {

    Text stationKey = new Text();
    TripWritable tuple = new TripWritable();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        String duration = tokens[4];

        if(!tokens[7].equals("from_station_id") && !duration.contains("tripduration") && duration != null) {

            stationKey.set(tokens[7].substring(1, tokens[7].length()-1));

            double tripDuration = Double.parseDouble(duration);
            tuple.setMinTrip(tripDuration);
            tuple.setMaxTrip(tripDuration);
            tuple.setAvgTrip(tripDuration);
            tuple.setCount(1);

            context.write(stationKey, tuple);
        }
    }
}
