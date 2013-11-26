package hadoopnb;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrainMapper extends Mapper<LongWritable , Text , Text , Text> {   
    
    private static HashMap<String,String> minmax;
    

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        minmax = new HashMap<String,String>();
        Configuration conf = context.getConfiguration();
        try (SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(conf.get("in")), conf)) {
            Text key = new Text();
            Text value = new Text();
            while (reader.next(key, value)) {
                minmax.put(key.toString(), value.toString().trim());
            }
        } catch (IOException ex) {
             System.out.println(ex);
             System.out.println(conf.get("in"));
             ex.printStackTrace();
        }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineText = value.toString().trim();

        if (lineText.isEmpty()) {
            return;
        }
        
        String []featureSet = {"age","workclass","fnlwgt","education","education-num",
                "marital-status","occupation","relationship","race",
                "sex","capital-gain","capital-loss","hours-per-week","native-country","class"
            };
        int[] indexes = {0,2,4,10,11,12}; 

        String[] features = lineText.split(",");
        long[] l = new long[6];
        int[] bucket = new int[6];
              
        for(int i = 0; i<6;i++){
            l[i] = Long.parseLong(features[indexes[i]].trim());
            long max = Long.parseLong(minmax.get(featureSet[indexes[i]]).split(",")[1]);        
            long min = Long.parseLong(minmax.get(featureSet[indexes[i]]).split(",")[0]);
            long bucketSize = (max - min)/10;
            bucket[i] = (int)((l[i] - min)/ bucketSize);
        }
        
        String cl = features[features.length-1];
        for(int i=0;i<features.length-1;i++){
            if (contains(indexes,i)>=0) {
                context.write(new Text(featureSet[i]+","+cl), new Text(""+bucket[contains(indexes,i)]));
            } else {
                context.write(new Text(featureSet[i]+","+cl), new Text(features[i]));
            }            
        }               
             
    }
    
    private static int contains(int [] array,int value){  
        for(int i =0; i<array.length;i++){
            if(array[i]==value)
                return i;
        }        
        return -1;
    }
}
