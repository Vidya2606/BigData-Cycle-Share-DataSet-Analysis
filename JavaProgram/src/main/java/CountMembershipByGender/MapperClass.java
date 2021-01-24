package CountMembershipByGender;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Count membership by gender
public class MapperClass extends Mapper <LongWritable, Text, Text, IntWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String user = tokens[9].substring(1,7);

        if("Member".equals(user) && !tokens[10].contains("gender") && !tokens[10].contains("Other")) {
            String gender = tokens[10].substring(1, tokens[10].length()-1);
            try {
                context.write(new Text(gender), new IntWritable(1));
            } catch (Exception e) {
            }
        }
    }
}
