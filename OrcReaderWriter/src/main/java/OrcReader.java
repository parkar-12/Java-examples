import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.apache.log4j.LogSF;
import org.apache.log4j.Logger;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;

import org.apache.hadoop.hive.ql.io.orc.Reader;


public class OrcReader implements Runnable {
    private static final Configuration CONF = new Configuration();
    private Path path;
    private Queue queue;
    public OrcReader(Path path, Queue queue){
        this.path=path;
        this.queue=queue;
    }
    @Override
    public void run() {
        System.out.println("Inside Thread{}"+Thread.currentThread().getName());
        try {
            Reader reader= OrcFile.createReader(path.getFileSystem(CONF),path);
            StructObjectInspector inspector = (StructObjectInspector)reader.getObjectInspector();
            System.out.println(reader.getMetadata());
            RecordReader records = reader.rows();
            Object row = null;
            List fields = inspector.getAllStructFieldRefs();
         /*   for(int i = 0; i < fields.size(); ++i) {
                System.out.print(((StructField)fields.get(i)).getFieldObjectInspector().g + '\t');
            }*/
            while (records.hasNext()) {
                row = records.next(row);
                List value_lst = inspector.getStructFieldsDataAsList(row);
                StringBuilder builder=new StringBuilder();
                for(Object value : value_lst){
                    if(value!= null){
                        builder.append(value.toString());
                    }
                    builder.append('\t');
                }
                StringBuilder builder1=new StringBuilder("Builder ");
                System.out.println(builder1.append(builder));
                queue.add(value_lst);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
