/**
 * Created by vikasjanardhanan on 3/30/17.
 */
import org.apache.hadoop.io.ArrayWritable;
public class ColumnContribWritableList extends ArrayWritable {
    public ColumnContribWritableList() {
        super(ColumnContrib.class);
    }
}
