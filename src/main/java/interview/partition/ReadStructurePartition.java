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

//
//        String fileNameStr=text.toString().substring(0,4);
//        int partitionNumber=map.size();
//        if (partitionNumber==0){
//            map.put(fileNameStr,0);
//            return  0;
//        }else if (map.get(fileNameStr)==null){
//            map.put(fileNameStr,partitionNumber);
//            return  partitionNumber;
//        }else {
//            return map.get(fileNameStr);
//        }
//
//        public int getPartition(Text key, IntWritable value, int numPartitions) {
//            // TODO Auto-generated method stub
//
//            if(numPartitions == 2){
//                String partitionKey = key.toString();
//                if(partitionKey.charAt(0) > 'b' )
//                    return 0;
//                else
//                    return 1;
//            } else if(numPartitions == 1)
//                return 0;
//            else{
//                System.err.println("WordCountExampleParitioner
//                        can only handle either 1 or 2 paritions");
//                return 0;
//            }
//        }

    }
}
