package Q4CountTripsPerStationPerYear;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecSortGroupComparator extends WritableComparator {

    public SecSortGroupComparator() {
        super(YearStationPair.class, true);
    }

    @Override
    public int compare(WritableComparable k1, WritableComparable k2) {
        YearStationPair key1 = (YearStationPair)k1;
        YearStationPair key2 = (YearStationPair)k2;

        return key1.compareTo(key2);
    }
}
