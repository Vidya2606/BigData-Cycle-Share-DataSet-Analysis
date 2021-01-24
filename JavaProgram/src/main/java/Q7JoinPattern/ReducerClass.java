package Q7JoinPattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerClass extends Reducer<Text, Text, Text, Text> {

    private static final Text EMPTY_TEXT = new Text();
    private List<Text> listA = new ArrayList<Text>();
    private List<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    @Override
    protected void setup(Context context) {
        joinType = context.getConfiguration().get("join.type");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        listA.clear();
        listB.clear();

        for(Text val : values)
        {
            if(val.charAt(0) == 'A')
                listA.add(new Text(val.toString().substring(1)));
            else if(val.charAt(0) == 'B')
                listB.add(new Text(val.toString().substring(1)));
        }
        executeJoinLogic(context);
    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {

        if(joinType.equalsIgnoreCase("rightouter"))
        {
            for(Text b: listB)
            {
                if(!listA.isEmpty())
                {
                    for(Text a : listA)
                        context.write(a, b);
                }
                else
                {
                    context.write(EMPTY_TEXT, b);
                }
            }
        }

        // if both the lists are not empty, perform two nested for loops and join each of the values together
//        if (joinType.equalsIgnoreCase("inner")) {
//            if (!listA.isEmpty() && !listB.isEmpty()) {
//                for (Text a : listA) {
//                    for (Text b : listB) {
//                        context.write(a, b);
//                    }
//                }
//            }
//        }
    }
}
