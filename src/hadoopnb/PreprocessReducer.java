package hadoopnb;


import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreprocessReducer extends Reducer<Text, LongWritable , Text, Text> {   
    private static HashMap<String,String> finalvalues;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       finalvalues = new HashMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long min=Long.MAX_VALUE;
        long max=Long.MIN_VALUE;
        for (LongWritable lw : values) {
            long num= lw.get();
            if (num>max) {
                max=num;
            }
            if (num<min) {
                min=num;
            }           
        }
        finalvalues.put(key.toString(), min + "," + max);
        
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String key : finalvalues.keySet() ) {                  
            context.write(new Text(key), new Text(finalvalues.get(key)));
        }    
        
    }
    
}
