import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Launcher {
    public static void main(String agrs[]){
        BlockingQueue<String> queue = new ArrayBlockingQueue<String>(1024);
        String inputPath="Data/input.txt";
        String outputPath="Data/output.txt";
        Reader reader = new Reader(queue,inputPath);
        Writer writer = new Writer(queue,outputPath);

        new Thread(reader).start();
        new Thread(writer).start();
    }
}
