package hadoopnb;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrainReducer extends Reducer<Text ,Text ,Text ,LongWritable>{
    
    HashMap<String,Integer> frequences;
   
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       frequences = new HashMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fc = key.toString();
        for(Text val : values){
            String outKey = fc+","+val.toString();
            if(!frequences.keySet().contains(outKey)){
                frequences.put(outKey, 1);
            }else{
                frequences.put(outKey,frequences.get(outKey)+1);
            }            
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String key :   frequences.keySet() ) {
            context.write(new Text(key), new LongWritable(frequences.get(key)));
        }
    }
    
}
