package elm.census.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class CensusReducer extends Reducer<Text, MapWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		
		for (MapWritable value : values) {
			 double totalPopul = ((DoubleWritable)value.get(new Text("totalPopln"))).get();
			 double totalMale = ((DoubleWritable)value.get(new Text("totalMale"))).get();
			 double totalFemale = ((DoubleWritable)value.get(new Text("totalFemale"))).get();
			 double totalMaleLessThan7 = ((DoubleWritable)value.get(new Text("totalMaleLessThan7"))).get();
			 double totalFemaleLessThan7 = ((DoubleWritable)value.get(new Text("totalFemaleLessThan7"))).get();
			 double totalMaleLiterate = ((DoubleWritable)value.get(new Text("totalMaleLiterate"))).get();
			 double totalFemaleLiterate = ((DoubleWritable)value.get(new Text("totalFemaleLiterate"))).get();
			 double totalMaleWorker = ((DoubleWritable)value.get(new Text("totalMaleWorker"))).get();
			 double totalFemaleWorker = ((DoubleWritable)value.get(new Text("totalFemaleWorker"))).get();
             
			 StringBuffer sb =new StringBuffer();
			 sb.append(",");
			 sb.append(calculatePercentage(totalMale,totalPopul));
			 sb.append(",");
			 sb.append(calculatePercentage(totalFemale,totalPopul));
			 sb.append(",");
			 sb.append(calculatePercentage(totalMaleLessThan7,totalMale));
			 sb.append(",");
			 sb.append(calculatePercentage(totalFemaleLessThan7,totalFemale));
			 sb.append(",");
			 sb.append(calculatePercentage(totalMaleLiterate,totalMale));
			 sb.append(",");
			 sb.append(calculatePercentage(totalFemaleLiterate,totalFemale));
			 sb.append(",");
			 sb.append(calculatePercentage(totalMaleWorker,totalMale));
			 sb.append(",");
			 sb.append(calculatePercentage(totalFemaleWorker,totalFemale));
			 context.write(key, new Text(sb.toString()));
			
			 
        }
	}
	/**
	 Method to calculate Percentage
	**/
	public double calculatePercentage(double obtained, double total) {
        return obtained * 100 / total;
    }
	
}
