import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class Reader implements Runnable {
    protected BlockingQueue<String> blockingQueue = null;
    private String inputPath;
    public Reader(BlockingQueue<String> blockingQueue,String inputPath){
        this.blockingQueue = blockingQueue;
        this.inputPath=inputPath;
    }

    public void run() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(new File(inputPath)));
            String buffer =null;
            while((buffer=br.readLine())!=null){
           //     System.out.println("Reading...");
                blockingQueue.put(buffer);
            //    Thread.sleep(500);
            }
            blockingQueue.put("EOF");  //When end of file has been reached

        } catch (FileNotFoundException e) {

            e.printStackTrace();
        } catch (IOException e) {

            e.printStackTrace();
        } catch(InterruptedException e){

        }finally{
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
