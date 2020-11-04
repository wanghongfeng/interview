package interview.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class ReadStructurePartition extends Partitioner<Text,Text> {
    public static final Map<String ,Integer> map=new HashMap<String, Integer>();
    @Override
    public int getPartition(Text text, Text fileName, int i) {
        String fileNameStr=fileName.toString().substring(0,4);
        if(fileNameStr.startsWith("fhvh")){
            return  1;
        }else if(fileNameStr.startsWith("gree")){
            return  2;
        }else if(fileNameStr.startsWith("yell")){
            return  3;
        }
        return  0;

    }
}
