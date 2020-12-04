import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.orc.NullMemoryManager;
import org.apache.orc.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.JDBCType.BIGINT;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.SNAPPY;

public class OrcWriter implements Runnable {
    @Override
    public void run() {
        Configuration conf = new Configuration(false);
        Writer writer=OrcFile.createWriter();

    /*    for (Map.Entry<String, String> entry : metadata.entrySet()) {
            writer.addUserMetadata(entry.getKey(), ByteBuffer.wrap(entry.getValue().getBytes(UTF_8)));
        }
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}
