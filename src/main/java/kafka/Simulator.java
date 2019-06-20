package kafka;

import org.apache.kafka.clients.producer.Producer;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class Simulator {
    public static final String path="Comments_jan-apr2018.csv";
    public static void main(String[] args) throws IOException, InterruptedException {
        //Read dataset
        File file = new File("data/"+path);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line,next="";
        Producer<String, String> producer = ProducerKafka.setConfig();
        //to skip header
        reader.readLine();
        //Produce first line
        line=reader.readLine();
        ProducerKafka.produce(producer,line);
        int count=1;
        long passed_time = Long.parseLong(line.split(",", -1)[5]);

        //Produce on Kafka
        while((next=reader.readLine())!=null && count<=60){
            count++;
            long firstApproveDate = Long.parseLong(line.split(",", -1)[5]);
            long nextApproveDate = Long.parseLong(next.split(",", -1)[5]);
            long time=(nextApproveDate-firstApproveDate);//Difference time between first time and the next
            float hourPassed = (float)(nextApproveDate - passed_time) / 3600;

            System.out.println("TIME "+hourPassed+"   ");
            System.out.println("ArticleID "+next.split(",",-1)[1]+"\n");
            TimeUnit.MILLISECONDS.sleep(time);

            ProducerKafka.produce(producer, next);
            line=next;


            }
        producer.close();

        }

}





