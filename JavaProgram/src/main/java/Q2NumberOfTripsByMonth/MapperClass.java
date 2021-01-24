package Q2NumberOfTripsByMonth;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper <LongWritable, Text, Text, IntWritable> {

    private String[] months = {"January", "February", "March", "April", "May", "June", "July", "August", "September",
                                "October", "November", "December"};

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        String date = fields[1].substring(1);

        if (!fields[1].contains("starttime")) {
            Integer month = Integer.parseInt(date.split("/")[0]);
            String [] dateParts = date.split("/");
            try {
                context.write(new Text(months[month-1] + "-" + dateParts[2].split(" ")[0]), new IntWritable(1));
            } catch (IOException e) {

            }
        }
    }
}
