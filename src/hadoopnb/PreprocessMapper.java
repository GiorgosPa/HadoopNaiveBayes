package hadoopnb;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreprocessMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String lineText = value.toString().trim();

        if (lineText.isEmpty()) {
            return;
        }

        String[] lineParts = lineText.split(",");
        long[] l = new long[6];
        l[0] = Long.parseLong(lineParts[0].trim());
        l[1] = Long.parseLong(lineParts[2].trim());
        l[2] = Long.parseLong(lineParts[4].trim());
        l[3] = Long.parseLong(lineParts[10].trim());
        l[4] = Long.parseLong(lineParts[11].trim());
        l[5] = Long.parseLong(lineParts[12].trim());          
        
        context.write(new Text("age"), new LongWritable(l[0]));  
        context.write(new Text("fnlwgt"), new LongWritable(l[1])); 
        context.write(new Text("education-num"), new LongWritable(l[2])); 
        context.write(new Text("capital-gain"), new LongWritable(l[3])); 
        context.write(new Text("capital-loss"), new LongWritable(l[4])); 
        context.write(new Text("hours-per-week"), new LongWritable(l[5])); 
    }
    
    
}
