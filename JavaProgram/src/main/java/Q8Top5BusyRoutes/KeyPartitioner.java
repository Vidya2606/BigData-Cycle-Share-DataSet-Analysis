package Q8Top5BusyRoutes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<FromToStationPair, IntWritable> {

    @Override
    public int getPartition(FromToStationPair key, IntWritable value, int numPartitions) {
        return key.getFromStationId().hashCode() % numPartitions;
    }
}
