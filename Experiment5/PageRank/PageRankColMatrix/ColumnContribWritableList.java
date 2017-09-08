import org.apache.hadoop.io.ArrayWritable;

/**
 * Created by vikasjanardhanan on 4/2/17.
 */

public class ColumnContribWritableList extends ArrayWritable {

    public ColumnContribWritableList() {
        super(ColumnContrib.class);
    }
}

