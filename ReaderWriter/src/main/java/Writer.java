import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;

public class Writer implements Runnable {

    protected BlockingQueue<String> blockingQueue = null;
    private String outputPath;
    public Writer(BlockingQueue<String> blockingQueue,String outputPath){
        this.blockingQueue = blockingQueue;
        this.outputPath=outputPath;
    }
    public void run() {
        PrintWriter writer = null;

        try {
            writer = new PrintWriter(new File(outputPath));
            while(true){
          //      System.out.println("Writing...");
                String buffer = blockingQueue.take();
                //Check whether end of file has been reached
                if(buffer.equals("EOF")){
                    break;
                }
                writer.println(buffer);
            }


        } catch (FileNotFoundException e) {

            e.printStackTrace();
        } catch(InterruptedException e){

        }finally{
            writer.close();
        }
    }
}
