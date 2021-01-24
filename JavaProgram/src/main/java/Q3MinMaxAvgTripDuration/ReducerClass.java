package Q3MinMaxAvgTripDuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer <Text, TripWritable, Text, TripWritable> {

    TripWritable output = new TripWritable();

    protected void reduce(Text key, Iterable <TripWritable> values, Context context) throws IOException, InterruptedException {

        double minTripDuration = Integer.MAX_VALUE;
        double maxTripDuration = Integer.MIN_VALUE;
        long count = 0;
        double sumOfDurations = 0;

        for(TripWritable val: values) {
            sumOfDurations += val.getAvgTrip() * val.getCount();
            count += val.getCount();

            if(val.getMinTrip() < minTripDuration) {
                minTripDuration = val.getMinTrip();
            }

            if(val.getMaxTrip() > maxTripDuration) {
                maxTripDuration = val.getMaxTrip();
            }
        }
        output.setMinTrip(minTripDuration);
        output.setMaxTrip(maxTripDuration);
        output.setAvgTrip(sumOfDurations / count);
        output.setCount(count);

        context.write(key, output);
    }
}
