package Q5Top5BusiestStations;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecSortGroupComparator extends WritableComparator {

    public SecSortGroupComparator() {
        super(MonthStationPair.class, true);
    }

    @Override
    public int compare(WritableComparable k1, WritableComparable k2) {
        MonthStationPair key1 = (MonthStationPair)k1;
        MonthStationPair key2 = (MonthStationPair) k2;
        return key1.compareTo(key2);
    }
}
