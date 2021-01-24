package Q8Top5BusyRoutes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Top10Reducer extends Reducer <FromToStationPair, IntWritable, Text, Text> {
    private PriorityQueue<RouteFreq> heap;

    protected void setup(Context context) throws IOException, InterruptedException {
        heap = new PriorityQueue<RouteFreq>(new Comparator<RouteFreq>() {
            public int compare(RouteFreq o1, RouteFreq o2) {
                return o2.freq - o1.freq;
            }
        });
    }

    @Override
    protected void reduce(FromToStationPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for(IntWritable val: values) {
            count += val.get();
        }

        heap.add(new RouteFreq(count, new FromToStationPair(key.getFromStationId(), key.getToStationId())));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        int count = 0;
        StringBuilder sb = new StringBuilder();
        while(!heap.isEmpty() && count < 10) {
            RouteFreq route = heap.poll();
            sb.append(route.key.getFromStationId()).append(" <-> ").append(route.key.getToStationId())
                    .append(":").append(route.freq).append("\n");
            count++;
        }
       context.write(new Text("Top 10 Routes"), new Text(sb.toString()));
    }

   class RouteFreq {
        int freq;
        FromToStationPair key;

        public RouteFreq(int freq, FromToStationPair key) {
            this.freq = freq;
           this.key = key;
        }
    }
}
