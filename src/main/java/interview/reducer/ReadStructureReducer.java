package interview.reducer;

import interview.bean.YellowGreenTripBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReadStructureReducer extends Reducer<Text, YellowGreenTripBean,NullWritable,Text> {
    @Override
    protected void reduce(Text key, Iterable<YellowGreenTripBean> values, Context context) throws IOException, InterruptedException {
        for (Object value :  values) {
            context.write(NullWritable.get(), new Text(value.toString()));
        }
    }
}
