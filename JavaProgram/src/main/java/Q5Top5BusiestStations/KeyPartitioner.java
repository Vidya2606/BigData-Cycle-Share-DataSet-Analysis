package Q5Top5BusiestStations;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner <MonthStationPair, IntWritable> {

    @Override
    public int getPartition(MonthStationPair key, IntWritable value, int numPartitions) {
        return key.getMonth().hashCode() % numPartitions;
    }
}
