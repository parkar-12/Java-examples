import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.LogSF;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class launcher {
    private static final Configuration CONF = new Configuration();
    public static void main(String args[]) throws IOException {
        String path="data/OrcInput";
        Path filepath= new Path(path);
        FileSystem hdfs = FileSystem.get(CONF);
        FileStatus[] status = hdfs.listStatus(filepath);
        Queue queue = new LinkedBlockingQueue();
        int i = 0;
        ExecutorService executor = Executors.newFixedThreadPool(6);
        for (FileStatus file : status) {
            System.out.println(file);
            System.out.println("Thread {} started: "+ i);
            if(!file.getPath().getName().equals("_SUCCESS")) {
                System.out.println("Reading file {} " + file.getPath().getName());
                executor.execute(new OrcReader(file.getPath(), queue));
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (!queue.isEmpty()){
            System.out.println(queue.poll());
        }
    }
}
