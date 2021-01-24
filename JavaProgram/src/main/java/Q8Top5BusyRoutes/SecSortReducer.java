package Q8Top5BusyRoutes;

        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.mapreduce.Reducer;

        import java.io.IOException;

public class SecSortReducer extends Reducer <FromToStationPair, IntWritable, FromToStationPair, IntWritable> {

    protected void reduce(FromToStationPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        for(IntWritable val: values) {
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }
}
