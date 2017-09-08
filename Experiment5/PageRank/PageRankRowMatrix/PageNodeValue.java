import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;

public class PageNodeValue implements Writable{
    public boolean isSink; // to check if it a dangling node or not
    public ArrayList<String> adjList;

    public PageNodeValue(){
        this.isSink = true;
        this.adjList = new ArrayList<String>();
    }

    @Override
    public  void write(DataOutput out) throws IOException{
        out.writeBoolean(isSink);
        out.writeInt(adjList.size());
        for (String node : adjList){
            out.writeUTF(node);
        }
    }

    public void readFields(DataInput in) throws IOException{
        this.isSink = in.readBoolean();
        this.adjList = new ArrayList<String>();
        int arrayLength = in.readInt();
        for(int i = 0; i< arrayLength; i++){
            this.adjList.add(in.readUTF());
        }
    }
}