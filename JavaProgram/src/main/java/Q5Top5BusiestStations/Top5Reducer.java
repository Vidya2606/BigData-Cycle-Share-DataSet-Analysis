package Q5Top5BusiestStations;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Top5Reducer extends Reducer <IntWritable, StationCountWrittable, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<StationCountWrittable> values, Context context) throws IOException, InterruptedException {
        List <StationCountWrittable> allStations = new ArrayList<StationCountWrittable>();

        for (StationCountWrittable value : values) {
            allStations.add(new StationCountWrittable(value.getStation(), value.getCount()));
        }

        int count = 0;
        StringBuilder stations = new StringBuilder();
        stations.append("");

        Collections.sort(allStations, new Comparator<StationCountWrittable>() {
            public int compare(StationCountWrittable o1, StationCountWrittable o2) {
                return o2.getCount() - o1.getCount();
            }
        });

        String prefix = "";
        while(count < 5 && count < allStations.size()) {
            StationCountWrittable station = allStations.get(count);
            stations.append(prefix).append(station.getStation());
            prefix = ", ";
            count++;
        }

        context.write(key, new Text(stations.toString()));
    }
}
