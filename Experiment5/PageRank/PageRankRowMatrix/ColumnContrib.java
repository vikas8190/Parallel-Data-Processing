/**
 * Created by vikasjanardhanan on 3/30/17.
 */
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.io.IOException;

public class ColumnContrib implements Writable{
    public long colNo;
    public double totalContrib;

    public ColumnContrib(){

    }

    public ColumnContrib(long colNo,double contribution){
        this.colNo=colNo;
        this.totalContrib=contribution;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeLong(colNo);
        dataOutput.writeDouble(totalContrib);
    }

    public void readFields(DataInput dataInput) throws IOException{
        this.colNo=dataInput.readLong();
        this.totalContrib=dataInput.readDouble();
    }
}
