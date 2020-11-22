import java.util.Queue;

public class Worker implements Runnable {

    Queue queue;
    public Worker(Queue queue){
        this.queue=queue;
    }
    public void run() {
        while(!queue.isEmpty()){
            System.out.println(queue.poll() + "processed by "+ Thread.currentThread().getName());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
