package Q6MostActiveAgeGroup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper <LongWritable, Text, MemberandAge, IntWritable> {

    MemberandAge tuple = new MemberandAge();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        String member = tokens[9].substring(1, tokens[9].length()-1);
        if (member.contains("Short-Term Pass Holder")) {
            return;
        }

        try {
            String birthyearString = tokens[11];
            if (!tokens[9].contains("usertype") && member != "" && !birthyearString.contains("birthyear") && birthyearString != "") {
                String year = tokens[1].substring(1);
                String[] dateParts = year.split("/");
                int currentyear = Integer.parseInt(dateParts[2].split(" ")[0]);
                int birthyear = Integer.parseInt(birthyearString);
                int diff = currentyear - birthyear;
                if (diff < 30) {
                    tuple.setAgeBand("[LT-30)");
                } else if (diff >= 30 && diff < 40) {
                    tuple.setAgeBand("[30-40)");
                } else if (diff >= 40 && diff < 50) {
                    tuple.setAgeBand("[40-50)");
                } else {
                    tuple.setAgeBand("[50+)");
                }
                context.write(tuple, new IntWritable(1));
            }
        } catch (Exception e) {
            // Skipping exceptions caused by data formatting
        }

    }
}
