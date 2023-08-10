package termproject.taxitime;

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

public class AvgTaxiTime{
	public static class taxiTimeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private Text flightDetails = new Text();
		private DoubleWritable totalTaxiTime = new DoubleWritable();
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
			//To Ignore the header from the data set
			if(line.startsWith("Year")){
				return;
			}
			
			String[] fields = value.toString().split(",");
			
			String airportCode = fields[16];
			String carrierCode = fields[8];
			String flightNum = fields[9];
			double taxiIn;
			if(fields[9].equals("NA")){
				taxiIn = 0.0;
			}
			else{
				taxiIn = Double.parseDouble(fields[9]);
			}
			double taxiOut;
			if(fields[9].equals("NA")){
				taxiOut = 0.0;
			}
			else{
				taxiOut = Double.parseDouble(fields[9]);
			}
			
			double totalTaxi = taxiIn + taxiOut;
			String flightInfo = airportCode + "-" + carrierCode + flightNum;
			
			flightDetails.set(flightInfo);
			totalTaxiTime.set(totalTaxi);
			
			context.write(flightDetails, totalTaxiTime);
			
			
		}
		
	}
	
	public static class taxiTimeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			int totalFlights = 0;
			double taxiTimeTotal = 0.0;
			
			for(DoubleWritable val: values){
				totalFlights++;
				taxiTimeTotal = taxiTimeTotal + val.get();
					
			}
			
			double averageTaxiTime = (double) taxiTimeTotal / totalFlights;
			context.write(key, new DoubleWritable(averageTaxiTime));
		}
		
	}
	
	public static class PostProcess{
		public static void main(String[] args) throws Exception{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			Path outputPath = new Path(args[0]);
			
			Map<Double, String> taxiTimeMap = new TreeMap<>();
			
			FileStatus[] files = fs.globStatus(new Path(outputPath, "part-r-*"));
			
			//Getting the contents of the output file from reducer job to a Map for sorting
			for (FileStatus file : files) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    String airline = parts[0];
                    double probability = Double.parseDouble(parts[1]);
                    taxiTimeMap.put(probability, airline);
                }
                reader.close();
            }
			
			//Perform the sort operation and load a text file
			String txtFilePath = outputPath+"/avgTaxiTime_highest_lowest.txt";
			Path hdfsTextFilePath = new Path(txtFilePath);
			
			try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(hdfsTextFilePath)))) {
			int count=0;
			for(Map.Entry<Double, String> entry: taxiTimeMap.entrySet()){
				if(count < 3 || count >= taxiTimeMap.size() - 3){
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
        Job job = Job.getInstance(conf, "AvgTaxiTime");
        job.setJarByClass(AvgTaxiTime.class);
        job.setMapperClass(taxiTimeMapper.class);
        job.setReducerClass(taxiTimeReducer.class);
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
