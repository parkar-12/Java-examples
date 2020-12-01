import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

import org.apache.hadoop.hive.ql.io.orc.Reader;


public class OrcReader implements Runnable {
    private static final Configuration CONF = new Configuration();
    private Path path;
    private Queue queue;
    private Queue jsonStrings;
    public OrcReader(Path path, Queue queue,Queue jsonStrings){
        this.path=path;
        this.queue=queue;
        this.jsonStrings=jsonStrings;
    }
    @Override
    public void run() {
        System.out.println("Inside Thread{}"+Thread.currentThread().getName());
        try {
            Reader reader= OrcFile.createReader(path.getFileSystem(CONF),path);
            StructObjectInspector inspector = (StructObjectInspector)reader.getObjectInspector();
        //    System.out.println(reader.getMetadata());
            RecordReader records = reader.rows();
            Object row = null;
            List fields = inspector.getAllStructFieldRefs();
         //   System.out.println("header "+((StructObjectInspector) reader.getObjectInspector()));
            List header = new ArrayList();
           for(int i = 0; i < fields.size(); ++i) {
            //    System.out.print(((StructField)fields.get(i)).getFieldName() + '\t');
               header.add(((StructField)fields.get(i)).getFieldName());
            }
            while (records.hasNext()) {
                row = records.next(row);
                List value_lst = inspector.getStructFieldsDataAsList(row);
             //   StringBuilder builder=new StringBuilder();
                List<Map<String, Object>> lis = new LinkedList<>();
                Map<String, Object> map = new HashMap<>();
                for(int j=0;j<value_lst.size();j++){

                    if(value_lst.get(j) != null){
                        map.put((String) header.get(j),value_lst.get(j));
                      //  builder.append(value.toString());
                        lis.add(map);
                    }
                    //builder.append('\t');
                }
                queue.add(map);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
