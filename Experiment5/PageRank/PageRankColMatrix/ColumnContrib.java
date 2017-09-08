/**
 * Created by vikasjanardhanan on 4/2/17.
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ColumnContrib implements Writable {
    public long rowNo;
    public double totalContrib;
    public boolean isContributer;
    public boolean isPageRank;
    public boolean isDangling;

    public ColumnContrib(){

    }
    public ColumnContrib(long rowNo, double totalContrib){
        this.rowNo = rowNo;
        this.totalContrib = totalContrib;
        this.isContributer = true;
        this.isPageRank = false;
        this.isDangling = false;
    }

    public ColumnContrib(double pagerank){
        this.isContributer = false;
        this.totalContrib = pagerank;
        this.isPageRank = true;
        this.isDangling = false;
        this.rowNo = -1;
    }

    public ColumnContrib (long danglingNodeNumber){
        this.isContributer = false;
        this.totalContrib=0.0;
        this.isPageRank = false;
        this.isDangling = true;
        this.rowNo = danglingNodeNumber;
    }

    @Override
    public	void write(DataOutput out) throws IOException{
        out.writeLong(rowNo);
        out.writeDouble(totalContrib);
        out.writeBoolean(isContributer);
        out.writeBoolean(isPageRank);
        out.writeBoolean(isDangling);

    }

    public void readFields(DataInput in) throws IOException{
        this.rowNo = in.readLong();
        this.totalContrib = in.readDouble();
        this.isContributer = in.readBoolean();
        this.isPageRank = in.readBoolean();
        this.isDangling = in.readBoolean();
    }
}
