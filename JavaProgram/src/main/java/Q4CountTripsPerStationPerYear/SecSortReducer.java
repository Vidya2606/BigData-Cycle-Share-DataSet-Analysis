package Q4CountTripsPerStationPerYear;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecSortReducer extends Reducer<YearStationPair, IntWritable, YearStationPair, IntWritable> {

    protected void reduce(YearStationPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for (IntWritable val : values) {
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }
}
