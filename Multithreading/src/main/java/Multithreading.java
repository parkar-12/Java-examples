import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Multithreading {
    public static void main(String args[]) throws InterruptedException {
        System.out.println("This is Multithreading");
        Queue queue = new LinkedBlockingQueue();
        for(int i=0;i<10;i++){
            queue.add(i);
        }
        Worker worker= new Worker(queue);
        Thread t1= new Thread(worker);
        Thread t2= new Thread(worker);
        t1.start();
        t1.join();
        t2.start();
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
