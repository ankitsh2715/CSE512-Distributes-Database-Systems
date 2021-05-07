import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class equijoin {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			//get row and column values from input
			String row = value.toString();
			String[] dataItems = row.split(",");

			Text keyData = new Text();
			Text valueData = new Text();

			//key is the joinColumn which is 2nd element (index=1) in input row 
			keyData.set(dataItems[1]);
			//value is entire row input
			valueData.set(row);

			//add key-value pair to output
			output.collect(keyData, valueData);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			boolean setFlag = false;

			String row = "";
			String tableName1 = "";
			String[] dataItems;
			String currTableName;

			Set<String> table1_Set = new HashSet<>();
			Set<String> table2_Set = new HashSet<>();

			while (values.hasNext()) {

				row = values.next().toString();
				dataItems = row.split(",");

				//table name is the first element of the row
				currTableName = dataItems[0];
				// this if block only executes once to set tableName1
				if (!setFlag) {
					tableName1 = currTableName;
					setFlag = true;
				}

				//all rows with currTableName==tableName1 will be added to set1 and other rows will be added to set2
				if (currTableName.equals(tableName1)) {
					//all rows with tableName1 is added to set1
					table1_Set.add(row);
				} else {
					//all rows currTableName!=tableName1 (which is the 2nd table in inputs) is added to set2
					table2_Set.add(row);
				}
			}

			Text rowText = new Text();
			for (String t1 : table1_Set) {
				for (String t2 : table2_Set) {
					//all combinations of rows in table1 and table2 added to output
					rowText.set(t1 + ", " + t2);
					output.collect(rowText, null);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(equijoin.class);

		//set name for the job
		conf.setJobName("equijoin");
		
		//set data type of key-value output
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		//name of mapper and reducer class
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		//input and output paths that are provided by cmd line arguments
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		//run job with config set above
		JobClient.runJob(conf);
	}
}