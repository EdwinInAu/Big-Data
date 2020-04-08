//
// Project 1 - MapReduce
//
// Authors:
//
// Chongshi Wang
//

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Ass {
	
	/**
 	*
 	*custom arraywritable (arrayMovieAndRate): The output of this function is in the form as [(movie1,rate1),(movie2,rate2)]
 	*And each element in the array is in the form as (movie1,rate1) -> (movieAndRate custom Writable)
 	*The toString function of arrayMovieAndRate: each (movie1,rate1) is followed by "," 
 	*The “,” of the last element is deleted in the final process, and add "[" and "]" to the most left and right of the string
 	*Finally, output the final result
 	*/

	public static class arrayMovieAndRate extends ArrayWritable{
		public arrayMovieAndRate(){
			super(movieAndRate.class);
		}
		public arrayMovieAndRate(movieAndRate[] values) {
			super(movieAndRate.class, values);
		}
		public movieAndRate[] get() {
			return (movieAndRate[]) super.get();
		}
		public String toString() {
			movieAndRate[] values = get();
			String result = "";
			for (movieAndRate element : values) {
				result += element.toString() + ",";
			}
			result = result.substring(0, result.length() - 1);
			result = "[" + result + "]";
			return result;
		}	
	}
		
	/**
 	*
 	*custom arraywritable (arrayUserAndRates): The output of this function is in the form as [(user1,rate1,rate2),(user2,rate3,rate4)]
 	*And each element in the array is in the form as (user1,rate1,rate2) -> (userAndRates custom Writable)
 	*The toString function of arrayUserAndRates: each (user1,rate1,rate2) is followed by "," 
 	*The “,” of the last element is deleted in the final process, and add "[" and "]" to the most left and right of the string
 	*Finally, output the final result
 	*/
		
	public static class arrayUserAndRates extends ArrayWritable{
		public arrayUserAndRates() {
			super(userAndRates.class);
		}
		public arrayUserAndRates(userAndRates[] values) {
			super(userAndRates.class, values);
		}
		public userAndRates [] get(){
			return (userAndRates[])super.get();
		}
		public String toString() {
			userAndRates[] values = get();
			String result = "";
			for(userAndRates element : values) {
				result += element.toString() + ",";
			}
			result = result.substring(0,result.length() -1);
			result = "[" + result + "]";
			return result;
		}
	}

	/**
	*
 	*custom writable (movieAndRate): The output of this function is in the form as (movie1,rate1)
 	*And this custom writable has two elements: (Text)movie and (IntWritable)rate
 	*The toString function of movieAndRate: "(" + movie + "," + rate + ")"
 	*Finally, output the final result (movie,rate)
 	*/

	public static class movieAndRate implements Writable{
			
		private Text movie;
		private IntWritable rate;
			
		public movieAndRate(){
			this.movie = new Text();
			this.rate = new IntWritable();
		}
			
		public movieAndRate(Text movie, IntWritable rate) {
			super();
			this.movie = movie;
			this.rate = rate;
		}
			
		public Text getMovie() {
			return movie;
		}

		public void setMovie(Text movie) {
			this.movie = movie;
		}

		public IntWritable getRate() {
			return rate;
		}

		public void setRate(IntWritable rate) {
			this.rate = rate;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie.readFields(data);
			this.rate.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movie.write(data);
			this.rate.write(data);
		}

		@Override
		public String toString() {
		String movieAndRatePair = "";
		movieAndRatePair += "(" + this.movie.toString() + "," + this.rate.toString() + ")";
			return movieAndRatePair;
		}
	}
		
	/**
	*
 	*custom writable (movies): The output of this function is in the form as (movie1,movie2)
 	*And this custom writable has two elements: (Text)movie1 and (Text)movie2
 	*The toString function of movies: "(" + movie1 + "," + movie2 + ")"
 	*The compareTo function of movies: ----------------
 	*Finally, output the final result (movie1,movie2)
 	*/
	
	public static class movies implements WritableComparable<movies>{
			
		private Text movie1;
		private Text movie2;
			
		public movies(){
			this.movie1 = new Text();
			this.movie2 = new Text();
		}
			
		public movies(Text movie1, Text movie2) {
			super();
			this.movie1 = movie1;
			this.movie2 = movie2;
		}
			
		public Text getMovie1() {
			return movie1;
		}

		public void setMovie1(Text movie1) {
			this.movie1 = movie1;
		}

		public Text getMovie2() {
			return movie2;
		}

		public void setMovie2(Text movie2) {
			this.movie2 = movie2;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie1.readFields(data);
			this.movie2.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movie1.write(data);
			this.movie2.write(data);
		}

		@Override
		public String toString() {
			String moviePairs = "";
			moviePairs += "(" + this.movie1.toString() + "," + this.movie2.toString() + ")";
			return moviePairs;
		}

		public int compareTo(movies value) {
			if (movie1.compareTo(value.movie1) == 0) {
				return movie2.compareTo(value.movie2);
			}
			else {
				return movie1.compareTo(value.movie1);
			}
		}	
	}
	
	/**
	*
 	*custom writable (userAndRates): The output of this function is in the form as (user1,rate1,rate2)
 	*And this custom writable has three elements: (Text)user1 , (IntWritable)rate1 and (IntWritable)rate2
 	*The toString function of movies: "(" + user1 + "," + rate1 + "," + rate2 + ")"
 	*Finally, output the final result (user1,rate1,rate2)
 	*/
		
	public static class userAndRates implements Writable{
		private Text user;
		private IntWritable rate1;
		private IntWritable rate2;
			
		public userAndRates(){
			this.user = new Text();
			this.rate1 = new IntWritable();
			this.rate2 = new IntWritable();
		}
			
		public userAndRates(Text user, IntWritable rate1, IntWritable rate2){
			super();
			this.user = user;
			this.rate1 = rate1;
			this.rate2 = rate2;
		}

		public Text getUser() {
			return user;
		}

		public void setUser(Text user) {
			this.user = user;
		}
			
	    public IntWritable getRate1() {
			return rate1;
		}
		
		public void setRate1(IntWritable rate1) {
			this.rate1 = rate1;
		}
		
		public IntWritable getRate2() {
			return rate2;
		}
		
		public void setRate2(IntWritable rate2) {
			this.rate2 = rate2;
		}
		
		@Override
		public void readFields(DataInput data) throws IOException {
			this.user.readFields(data);
			this.rate1.readFields(data);
			this.rate2.readFields(data);
		}
		
		@Override
		public void write(DataOutput data) throws IOException {
			this.user.write(data);
			this.rate1.write(data);
			this.rate2.write(data);	
		}
			
		public String toString() {
			String uARates = "";
			uARates += "(" + this.user.toString() + "," + this.rate1.toString() + "," + this.rate2.toString() + ")";
			return uARates;
		}	
	}
		
	/**
	*
 	*First Mapper: The output of this mapper is in the form as user(key) and (movie,rate)(value)
 	*Firstly, the input data in the txt is splited by "::"
 	*part[0] : user;  part[1]: movie; part[2]: rate
 	*Then, the part[0](user) becomes the key of the first mapper
 	*the part[1](movie) and part[2](rate) are added to the movieAndRate(custom writable)
 	*(movie,rate) becomes the value
 	*Finally, output the final result: user & (movie,rate)
 	*/

	public static class FirstMapper extends Mapper<LongWritable,Text,Text,movieAndRate>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, movieAndRate>.Context context)
			throws IOException, InterruptedException {
				String [] row = value.toString().split("::");
				Text user = new Text(row[0]);
				Text movie = new Text(row[1]);
				IntWritable rate = new IntWritable (Integer.parseInt(row[2]));
				movieAndRate mAndRPair = new movieAndRate();
				mAndRPair.setMovie(movie);
				mAndRPair.setRate(rate);
				context.write(user, mAndRPair);
		}	
	}
		
	/**
	*
 	*First Reducer: The input of this reducer is in the form as user (key) and (movie1,rate1) (value)
 	*Firstly, all input values are added to the Arraylist list1
 	*Then, list1(Arraylist) -> list2 (array) -> list3 (arrayMovieAndRate)
 	*Finally, output the final result: user & [(movie1,rate1),(movie2,rate2)]
 	*/

	public static class FirstReducer extends Reducer<Text,movieAndRate,Text, arrayMovieAndRate>{

		@Override
		protected void reduce(Text key, Iterable<movieAndRate> values,
			Reducer<Text, movieAndRate, Text, arrayMovieAndRate>.Context context)
			throws IOException, InterruptedException {
				ArrayList<movieAndRate> list1 = new ArrayList<movieAndRate>();
				for(movieAndRate element : values) {
					movieAndRate tmp = new movieAndRate();
					tmp.setMovie(new Text(element.getMovie().toString()));
					tmp.setRate(new IntWritable(element.getRate().get()));
					list1.add(tmp);
				}
				movieAndRate[] list2 = new movieAndRate[list1.size()];
				list1.toArray(list2);
				arrayMovieAndRate list3 = new arrayMovieAndRate(list2);
				context.write(key,list3);
		}
	}
	
	/**
	*
 	*Second Mapper: The input of this mapper is in the form as user (key) and [(movie1,rate1),(movie2,rate2)] (value)
 	*Firstly, all input values(input type: movieAndRate) are added to list1
 	*if the length of list1 > 1: enter the next step
 	* list1 -> list2 -> String list 3
 	*Then, sort the list 3 (effect: list is sorted from small to large according to the movie values)
 	*the elements of list3: (movie,rate) -> movie,rate
 	*two for loops: firstly split movie and rate by "," 
 	*two movie counterparts(index a, index b) become movies(custom writable) -> (movie1,movie2)
 	*two rate counterparts(index a, index b) and input key of mapper(user) become userAndRates(custom writable) -> (user1,rate1,rate2)
 	*Finally, output the final result: (movie1,movie2) (key) & (user1,rate1,rate2) (value)
 	*/

	public static class SecondMapper extends Mapper<Text,arrayMovieAndRate,movies,userAndRates>{

		@Override
		protected void map(Text key, arrayMovieAndRate value,
			Mapper<Text, arrayMovieAndRate, movies, userAndRates>.Context context)
			throws IOException, InterruptedException {
				movieAndRate[] list1 = (movieAndRate[]) value.toArray();
				int length = list1.length;
				if(length > 1) {
					List<movieAndRate> list2 = Arrays.asList(list1);
					String[] list3 = new String[list2.size()];
					int num = list2.size();
					int i = 0;
					for(movieAndRate val: list2) {
						list3[i] = val.toString();
						i = i + 1;
					}
					Arrays.sort(list3);
					int l = list3.length;
					for (int k =0;k<l;k++) {
						list3[k] = list3[k].replaceAll("\\(|\\)", "");
					}
				    for(int a=0;a< l-1;a++){
			            for(int b=a+1;b< l;b++){   
			             	String[] str1 = list3[a].split(",");
			             	String[] str2 = list3[b].split(",");
			            	movies movieP = new movies();
							movieP.setMovie1(new Text(str1[0]));
							movieP.setMovie2(new Text(str2[0]));
							userAndRates uAndRates = new userAndRates();
							uAndRates.setUser(key);
							uAndRates.setRate1(new IntWritable(Integer.parseInt(str1[1])));
							uAndRates.setRate2(new IntWritable(Integer.parseInt(str2[1])));
			            	context.write(movieP,uAndRates);
			        	}
			    	}
				}
		}	
	}
		
	/**
	*
 	*Second Reducer: The input of this reducer is in the form as (movie1,movie2) (key) and (user1,rate1,rate2) (value)
 	*Firstly, all input values are added to the Arraylist list1
 	*Then, list1(Arraylist) -> list2 (array) -> list3 (arrayUserAndRates)
 	*Finally, output the final result: (movie1,movie2) (key) and [(user1,rate1,rate2),(user2,rate3,rate4)](value)
 	*/

	public static class SecondReducer extends Reducer<movies,userAndRates,movies,arrayUserAndRates>{

		@Override
		protected void reduce(movies key, Iterable<userAndRates> values,
			Reducer<movies, userAndRates, movies, arrayUserAndRates>.Context context)
			throws IOException, InterruptedException {
				ArrayList<userAndRates> list1 = new ArrayList<userAndRates>();
				for(userAndRates element : values) {
					userAndRates e = new userAndRates();
					e.setUser(new Text(element.getUser().toString()));
					e.setRate1(new IntWritable(element.getRate1().get()));
					e.setRate2(new IntWritable(element.getRate2().get()));
					list1.add(e);
				}
				userAndRates[] list2 = new userAndRates[list1.size()];
				list1.toArray(list2);
				arrayUserAndRates list3 = new arrayUserAndRates(list2);
				context.write(key,list3);
		}	
	}
	
	/**
	*
 	*the main function
 	*reference: https://stackoverflow.com/questions/38111700/chaining-of-mapreduce-jobs#answer-38113499
 	*(the main fuction of Chaining of two mapreduce jobs)
 	*/

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Path out = new Path(args[1]);
		Job job1 = Job.getInstance(conf, "FirstStep");
		job1.setJarByClass(Ass.class);
	    job1.setMapperClass(FirstMapper.class);
		job1.setReducerClass(FirstReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(movieAndRate.class);
	    job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(arrayMovieAndRate.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
		FileSystem hdfs = FileSystem.get(conf);
		Path output = new Path(args[1]);

		if(hdfs.exists(output)){
			hdfs.delete(output,true);
		}
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
			
		Job job2 = Job.getInstance(conf, "SecondStep");
		job2.setJarByClass(Ass.class);
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);
		job2.setMapOutputKeyClass(movies.class);
		job2.setMapOutputValueClass(userAndRates.class);
		job2.setOutputKeyClass(movies.class);
		job2.setOutputValueClass(arrayUserAndRates.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(out, "out1"));
		FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}