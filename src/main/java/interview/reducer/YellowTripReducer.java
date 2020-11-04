package interview.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class YellowTripReducer extends Reducer<Text, Object,NullWritable,Text> {
    @Override
    protected void reduce(Text key, Iterable<Object> values, Context context) throws IOException, InterruptedException {

        for (Object value :  values) {
            context.write(NullWritable.get(), new Text(value.toString()));
        }
    }
}
