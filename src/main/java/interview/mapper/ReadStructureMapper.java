package interview.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReadStructureMapper extends Mapper<LongWritable,Text,Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //首行跳过
        if (!(key.toString().equals("0"))) {
            return;
        }
        //提取文件名
        String fileNameFull = ((FileSplit) context.getInputSplit()).getPath().toString();
        //提取文件名中新旧格式范围
        int endint=fileNameFull.indexOf(".csv");
        int beginint=fileNameFull.indexOf("trip data/");

        String fileName=fileNameFull.substring(beginint+10,endint);
        context.write(new Text(fileName), new Text(value.toString()));

    }
}
