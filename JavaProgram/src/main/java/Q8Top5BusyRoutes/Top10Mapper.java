package Q8Top5BusyRoutes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top10Mapper extends Mapper <LongWritable, Text, FromToStationPair, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] keyVals = value.toString().split(",");

        String fromStation = "";
        String[] keyParts = keyVals[0].split("\\s+");
        if (keyParts.length > 1) {
            String prefix = "";
            for(String keyPart: keyParts) {
                fromStation = fromStation + prefix + keyPart;
                prefix = " ";
            }
        } else {
            fromStation = keyVals[0];
        }

        String[] tokens = keyVals[1].trim().split("\\s+");
        int val = Integer.parseInt(tokens[tokens.length-1]);
        String toStation = "";

        if (tokens.length > 2) {
            String prefix = "";
            for(int i = 0; i < tokens.length - 2; i++) {
                toStation = toStation + prefix + tokens[i];
                prefix = " ";
            }
        } else {
            toStation = tokens[0];
        }

        FromToStationPair outkey = new FromToStationPair();
        outkey.setFromStationId(fromStation);
        outkey.setToStationId(toStation);

        context.write(outkey, new IntWritable(val));
    }
}
