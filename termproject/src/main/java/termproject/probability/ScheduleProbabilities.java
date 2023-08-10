package termproject.probability;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ScheduleProbabilities{
	public static class SchProbMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		
		private final static DoubleWritable onSchedule = new DoubleWritable(1.0);
		private final static DoubleWritable notOnSchedule = new DoubleWritable(0.0);
		private Text airline = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			//To Ignore the header from the data set
			if(line.startsWith("Year")){
				return;
			}
			
			String[] fields = value.toString().split(",");
			//Capturing the required fields for the probability calculation
			String airlineCode = fields[8];
			String depTime = fields[4];
			String schDepTime = fields[5];
			
			
			//Check and write 1 or 0 along with the carrier code depending on the departure time
			if(depTime.equals(schDepTime)){
				airline.set(airlineCode);
				context.write(airline, onSchedule);
			}
			else {
				airline.set(airlineCode);
				context.write(airline, notOnSchedule);
			}
		}
		
	}
	
	public static class SchProdReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			int totalFlights = 0;
			double flightsOnSch = 0.0;
			
			for(DoubleWritable val: values){
				totalFlights++;
				if(val.get() == 1){
					flightsOnSch = flightsOnSch + val.get();
				}
					
			}
			
			double probability = (double) flightsOnSch / totalFlights;
			context.write(key, new DoubleWritable(probability));
		}
		
	}
	
	public static class PostProcess{
		public static void main(String[] args) throws Exception{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			Path outputPath = new Path(args[0]);
			
			Map<Double, String> probabilityMap = new TreeMap<>();
			
			FileStatus[] files = fs.globStatus(new Path(outputPath, "part-r-*"));
			
			//Getting the contents of the output file from reducer job to a Map for sorting
			for (FileStatus file : files) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    String airline = parts[0];
                    double probability = Double.parseDouble(parts[1]);
                    probabilityMap.put(probability, airline);
                }
                reader.close();
            }
			
			//Perform the sort operation and load a text file
			String txtFilePath = outputPath+"/probabilities_highest_lowest.txt";
			Path hdfsTextFilePath = new Path(txtFilePath);
			
			try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(hdfsTextFilePath)))) {
			int count=0;
			for(Map.Entry<Double, String> entry: probabilityMap.entrySet()){
				if(count < 3 || count >= probabilityMap.size() - 3){
					//System.out.println(entry.getValue() + ": " + entry.getKey());
					String outputLine = entry.getValue() + ": " + entry.getKey();
                    writer.write(outputLine);
                    writer.newLine();
				}
				count++;
			}
		}catch (Exception e) {
            e.printStackTrace();
        }
		
		}
		
		
		
		
	}
	
	
	 public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ScheduleProbabilities");
        job.setJarByClass(ScheduleProbabilities.class);
        job.setMapperClass(SchProbMapper.class);
        job.setReducerClass(SchProdReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (job.waitForCompletion(true)) {
            PostProcess.main(new String[] { args[1] }); // Run the post-processing step
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}