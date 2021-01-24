package Q4CountTripsPerStationPerYear;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<YearStationPair, IntWritable> {

    @Override
    public int getPartition(YearStationPair key, IntWritable value, int numPartitions) {
        return key.getYear().hashCode() % numPartitions;
    }
}
