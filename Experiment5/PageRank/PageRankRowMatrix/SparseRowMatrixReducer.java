/**
 * Created by vikasjanardhanan on 3/30/17.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.ArrayList;

public class SparseRowMatrixReducer extends Reducer<LongWritable,ColumnContrib, LongWritable, ColumnContribWritableList>{

    private MultipleOutputs mos;

    public void setup(Context context){
        mos = new MultipleOutputs(context);

    }

    public void reduce(LongWritable key, Iterable<ColumnContrib> value, Context context) throws IOException, InterruptedException{
        ArrayList<ColumnContrib> contribs = new ArrayList<ColumnContrib>();
        for(ColumnContrib val : value){
            contribs.add(new ColumnContrib(val.colNo, val.totalContrib));
        }


        ColumnContrib [] contriblist;
        //= new ColumnContrib[arraylist.size()];

        contriblist = contribs.toArray(new ColumnContrib[contribs.size()]);
        //int i = 0;
        //for(ColumnContrib val :arraylist ){
        //    list[i++] = val;
        //}
        ColumnContribWritableList arrayWritableList = new ColumnContribWritableList();
        arrayWritableList.set(contriblist);
        mos.write(PageRankRowMatrix.MatrixFileID, new LongWritable(key.get()), arrayWritableList, PageRankRowMatrix.MatrixPathPrefix);
    }



    public void cleanup(Context context) throws IOException, InterruptedException{
        mos.close();
    }
}
