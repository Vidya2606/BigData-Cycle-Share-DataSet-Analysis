package Q6MostActiveAgeGroup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer <MemberandAge, IntWritable, MemberandAge, IntWritable> {

    MemberandAge output = new MemberandAge();

    protected void reduce(MemberandAge key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable val : values) {
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }
}
