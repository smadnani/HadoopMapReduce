package elm.census.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CensusMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] field = line.split(",");

		String stateName = field[0];
		String totalPopln = field[1];
		String totalMale = field[2];
		String totalFemale = field[3];
		String totalMaleLessThan7 = field[4];
		String totalFemaleLessThan7 = field[5];
		String totalMaleLiterate = field[6];
		String totalFemaleLiterate = field[7];
		String totalMaleWorker = field[8];
		String totalFemaleWorker = field[9];

		MapWritable mw = new MapWritable();

		if (!"STNAME".equalsIgnoreCase(stateName) && !"T_POPLN".equalsIgnoreCase(totalPopln)
				&& !"T_M_POPLN".equalsIgnoreCase(totalMale) && !"T_F_POPLN".equalsIgnoreCase(totalFemale)
				&& !"POPLN_M6".equalsIgnoreCase(totalMaleLessThan7)
				&& !"POPLN_F6".equalsIgnoreCase(totalFemaleLessThan7)
				&& !"M_LITERATE".equalsIgnoreCase(totalMaleLiterate)
				&& !"F_LITERATE".equalsIgnoreCase(totalFemaleLiterate)
				&& !"T_M_WORKER".equalsIgnoreCase(totalMaleWorker)
				&& !"T_F_WORKER".equalsIgnoreCase(totalFemaleWorker)) {
			mw.put(new Text("totalPopln"), new DoubleWritable(Double.parseDouble(totalPopln)));
			mw.put(new Text("totalMale"), new DoubleWritable(Double.parseDouble(totalMale)));
			mw.put(new Text("totalFemale"), new DoubleWritable(Double.parseDouble(totalFemale)));
			mw.put(new Text("totalMaleLessThan7"), new DoubleWritable(Double.parseDouble(totalMaleLessThan7)));
			mw.put(new Text("totalFemaleLessThan7"), new DoubleWritable(Double.parseDouble(totalFemaleLessThan7)));
			mw.put(new Text("totalMaleLiterate"), new DoubleWritable(Double.parseDouble(totalMaleLiterate)));
			mw.put(new Text("totalFemaleLiterate"), new DoubleWritable(Double.parseDouble(totalFemaleLiterate)));
			mw.put(new Text("totalMaleWorker"), new DoubleWritable(Double.parseDouble(totalMaleWorker)));
			mw.put(new Text("totalFemaleWorker"), new DoubleWritable(Double.parseDouble(totalFemaleWorker)));

			context.write(new Text(stateName), mw);
		}
	}
}
