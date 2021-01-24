package Q8Top5BusyRoutes;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecSortGroupComparator extends WritableComparator {

    public SecSortGroupComparator() {
        super(FromToStationPair.class, true);
    }

    @Override
    public int compare(WritableComparable k1, WritableComparable k2) {
        FromToStationPair key1 = (FromToStationPair) k1;
        FromToStationPair key2 = (FromToStationPair) k2;
        return key1.compareTo(key2);
    }
}
