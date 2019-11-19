package map_reduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text keys = new Text();
    IntWritable values=new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

//        StringUtils.isBlank(line)

            //把“,”替换为“|”
            String[] words = line.split(" ");
            for (String word : words) {
                if (word !=null) {

                    this.keys.set(word);
                    context.write(this.keys,this.values);
                }
                }
        }







//    boolean isRightData(String info){
//        boolean flag = true;
//        String[] items = info.split(" ");
//        for(String item: items){
//            if (item.trim().isEmpty()){
//                flag = false;
//                break;
//            }
//        }
//        return flag;
//    }


















}
