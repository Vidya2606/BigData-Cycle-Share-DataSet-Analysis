package Q5Top5BusiestStations;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecSortComparator extends WritableComparator {

    public SecSortComparator() {
        super(MonthStationPair.class, true);
    }

    @Override
    public int compare(WritableComparable k1, WritableComparable k2) {
        MonthStationPair key1 = (MonthStationPair) k1;
        MonthStationPair key2 = (MonthStationPair) k2;

        int result = key1.compareTo(key2); // -1, 0, 1
        return result;
    }
}

