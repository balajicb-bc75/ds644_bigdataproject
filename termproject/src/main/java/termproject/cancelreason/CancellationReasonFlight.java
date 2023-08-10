package termproject.cancelreason;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class CancellationReasonFlight{
	public static class FlightCancellationMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static Log LOG = LogFactory.getLog(FlightCancellationMapper.class);
		private final static IntWritable one = new IntWritable(1);
		
		private Text reasonText = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			String cancellationCode = fields[22];
			
			String cancellationReason = mapCodeToReason(cancellationCode);
			if(cancellationReason != null){
					reasonText.set(cancellationReason);
					context.write(reasonText, one);
			}
		}
		
		private String mapCodeToReason(String cancellationCode){
			switch(cancellationCode){
					case "A":
						return "carrier";
					case "B":
						return "weather";
					case "C":
						return "NAS";
					case "D":
						return "security";
					case "NA":
						return "Not Applicable";
					default:
						return null;
				}
		}
		
		
	}
	
	public static class FlightCancellationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			
			for(IntWritable val: values){
				sum+=val.get();					
			}
			
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	public static class PostProcess{
		public static void main(String[] args) throws Exception{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			Path outputPath = new Path(args[0]);
			
			Map<Double, String> cancelReasonMap = new TreeMap<>();
			
			FileStatus[] files = fs.globStatus(new Path(outputPath, "part-r-*"));
			
			//Getting the contents of the output file from reducer job to a Map for sorting
			for (FileStatus file : files) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    String airline = parts[0];
                    double probability = Double.parseDouble(parts[1]);
                    cancelReasonMap.put(probability, airline);
                }
                reader.close();
            }
			
			//Perform the sort operation and load a text file
			String txtFilePath = outputPath+"/cancellation_reason.txt";
			Path hdfsTextFilePath = new Path(txtFilePath);
			
			try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(hdfsTextFilePath)))) {
			int count=0;
			for(Map.Entry<Double, String> entry: cancelReasonMap.entrySet()){
					//System.out.println(entry.getValue() + ": " + entry.getKey());
					String outputLine = entry.getValue() + ": " + entry.getKey();
                    writer.write(outputLine);
                    writer.newLine();
					count++;
			}
		}catch (Exception e) {
            e.printStackTrace();
        }
		
		}
		
	}
	
	 public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CancellationReasonFlight");
        job.setJarByClass(CancellationReasonFlight.class);
        job.setMapperClass(FlightCancellationMapper.class);
        job.setReducerClass(FlightCancellationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
