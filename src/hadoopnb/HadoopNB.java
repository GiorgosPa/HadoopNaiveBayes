package hadoopnb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HadoopNB extends Configured implements Tool {

    private final static String OUTPUT_PATH = "hdfs://roxanne:54310/user/hduser5/";
    private final static String DATAPATH = "hdfs://roxanne:54310/user/hduser5/adult.data";

    static {
        Configuration.addDefaultResource(("/usr/local/hadoop/conf/hdfs-site.xml"));
        Configuration.addDefaultResource(("/usr/local/hadoop/conf/core-site.xml"));
        Configuration.addDefaultResource(("/usr/local/hadoop/conf/mapred-site.xml"));
    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new HadoopNB(), args);
        } catch (Exception ex) {
            Logger.getLogger(HadoopNB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        try {

            FileSystem fs = FileSystem.get(conf);
            Job preprocess = new Job(conf);

            preprocess.setJarByClass(HadoopNB.class);
            preprocess.setJobName("Naive Bayes Preprocess");

            preprocess.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(preprocess, new Path(DATAPATH));
            SequenceFileOutputFormat.setOutputPath(preprocess, new Path(OUTPUT_PATH, "preprocess"));

            preprocess.setMapperClass(PreprocessMapper.class);
            preprocess.setReducerClass(PreprocessReducer.class);

            preprocess.setMapOutputKeyClass(Text.class);
            preprocess.setMapOutputValueClass(LongWritable.class);

            preprocess.setOutputKeyClass(Text.class);
            preprocess.setOutputValueClass(Text.class);

            preprocess.setOutputFormatClass(SequenceFileOutputFormat.class);

            try {
                preprocess.waitForCompletion(true);
            } catch (InterruptedException | ClassNotFoundException ex) {
                Logger.getLogger(HadoopNB.class.getName()).log(Level.SEVERE, null, ex);
            }

            

            conf.set("in", OUTPUT_PATH + "preprocess/part-r-00000");
            Job train = new Job(conf);

            train.setJarByClass(HadoopNB.class);
            train.setJobName("Naive Bayes Train");

            train.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(train, new Path(DATAPATH));
            SequenceFileOutputFormat.setOutputPath(train, new Path(OUTPUT_PATH, "model"));

            train.setMapperClass(TrainMapper.class);
            train.setReducerClass(TrainReducer.class);

            train.setMapOutputKeyClass(Text.class);
            train.setMapOutputValueClass(Text.class);

            train.setOutputKeyClass(Text.class);
            train.setOutputValueClass(LongWritable.class);

            train.setOutputFormatClass(SequenceFileOutputFormat.class);

            try {
                train.waitForCompletion(true);
            } catch (InterruptedException | ClassNotFoundException ex) {
                Logger.getLogger(HadoopNB.class.getName()).log(Level.SEVERE, null, ex);
            }


            this.classify(fs, conf, new Path(DATAPATH));


        } catch (IOException ex) {
            Logger.getLogger(HadoopNB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    public void classify(FileSystem fs, Configuration conf, Path path) {       
        String features =  {"age", "fnlwgt", "education-num", "capital-gain", 
                            "capital-loss", "hours-per-week"}
        int[] indexes = {0, 2, 4, 10, 11, 12};        

        HashMap<String, Integer> modelFrequencies = readModel(fs, conf);

        HashMap<String, String> minmax = new HashMap<>();

        try (SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(conf.get("in")), conf)) {
            Text key = new Text();
            Text value = new Text();
            while (reader.next(key, value)) {
                minmax.put(key.toString(), value.toString());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }



        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line = br.readLine();
            FSDataOutputStream out = fs.create(new Path("results.txt"));

            int num = 1;
            //System.out.println(line);
            
            while (line != null && !line.equals("")) {
                String[] values = line.split(",");
                long[] l = new long[6];
                int[] bucket = new int[6];

                for (int i = 0; i < 6; i++) {
                    try{
                        l[i] = Long.parseLong(values[indexes[i]].trim());
                        long max = Long.parseLong(minmax.get(features[i]).split(",")[1]);
                        long min = Long.parseLong(minmax.get(features[i]).split(",")[0]);
                        long bucketSize = (max - min) / 10;
                        bucket[i] = (int) ((l[i] - min) / bucketSize);
                        
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }                

                //count instances of each class
                int totalInstances = 0;
                int positiveInstances = 0;
                for (String mapKey : modelFrequencies.keySet()) {
                    if (mapKey.startsWith("age")) {
                        totalInstances += modelFrequencies.get(mapKey);
                    }
                    if (mapKey.startsWith("age, >50K")) {
                        positiveInstances += modelFrequencies.get(mapKey);
                    }
                }

                String[] positiveKeys = new String[values.length];
                String[] negativeKeys = new String[values.length];

                //featurename+class+value;
                for (int i = 0; i < values.length - 1; i++) {
                    if (contains(indexes, i)) {
                        positiveKeys[i] = featureSet[i] + ", >50K," + bucket[i];
                        negativeKeys[i] = featureSet[i] + ", <=50K," + bucket[i];
                    } else {
                        positiveKeys[i] = featureSet[i] + ", >50K," + values[i];
                        negativeKeys[i] = featureSet[i] + ", <=50K," + values[i];
                    }
                }

                double positive = Math.log((double)positiveInstances / (double)totalInstances);
                double negative = Math.log(1 - positive);
               
                //System.out.println("prob: " + positive);
                //System.out.println("positiveInstances: " + positiveInstances);
                //System.out.println("totalInstances: " + totalInstances);
                for (String k : positiveKeys) {
                    for (String s : modelFrequencies.keySet()) {
                        if (s.equals(k)) {
                            try {
                                positive += Math.log((double)modelFrequencies.get(s) / (double)positiveInstances);
                                //System.out.println(modelFrequencies.get(s) );
                            } catch (Exception e) {
                                positive += Math.log(0.0000001);
                                System.out.println("NULL!!!!!!!!!");
                            }
                        }
                    }
                }
                
                for (String k : negativeKeys) {
                    for (String s : modelFrequencies.keySet()) {
                        if (s.equals(k)) {
                            try {
                                negative += Math.log((double)modelFrequencies.get(s) / (double)(totalInstances - positiveInstances));
                                //System.out.println(modelFrequencies.get(s) );
                            } catch (Exception e) {
                                negative += Math.log(0.0000001);
                                System.out.println("NULL!!!!!!!!!");
                            }
                        }
                    }
                }
                
                //System.out.println(positive+"\t"+ negative);

                if (positive > negative) {
                    out.writeBytes("" + num + "," + ">50K\n");
                } else {
                    out.writeBytes("" + num + "," + "<=50K\n");
                }

                num++;
                line = br.readLine();
            }
            br.close();
            out.close();

        } catch (IOException ex) {
            ex.printStackTrace();
            System.out.println(ex);
        }

    }

    private static int contains(int[] array, int value) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) {
                return true;
            }
        }
        return false;
    }

    public HashMap<String, Integer> readModel(FileSystem fs, Configuration conf) {
        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(OUTPUT_PATH, "model/part-r-00000"), conf)) {
            HashMap<String, Integer> frequences = new HashMap<>();
            Text key = new Text();
            LongWritable value = new LongWritable();
            while (reader.next(key, value)) {
                frequences.put(key.toString(), (int) value.get());
            }

            return frequences;

        } catch (IOException ex) {
            System.out.println(ex);
            ex.printStackTrace();
            return null;
        }

    }
}
