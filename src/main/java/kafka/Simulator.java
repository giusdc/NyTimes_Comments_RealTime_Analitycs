package kafka;

import org.apache.kafka.clients.producer.Producer;
import scala.Int;

import java.io.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Simulator {
    private static final String path = "Comments_jan-apr2018.csv";

    public static void main(String[] args) throws IOException, InterruptedException {
        //Read dataset
        File file = new File("data/" + path);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line, next = "";
        Producer<String, String> producer = ProducerKafka.setConfig(args[0]);
        //to skip header
        reader.readLine();
        //Produce first line
        line = reader.readLine();
        System.out.println("ArticleID " + line.split(",", -1)[1]);
        long passed_time = Long.parseLong(line.split(",", -1)[5]);
        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(passed_time),
                        ZoneOffset.UTC.normalized());
        System.out.println("date " + triggerTime + "\n");
        ProducerKafka.produce(producer, line,"comments");
        //Produce on Kafka
        while ((next = reader.readLine()) != null) {
            if (checkTime(next)){
                long firstApproveDate = Long.parseLong(line.split(",", -1)[5]);
                long nextApproveDate = Long.parseLong(next.split(",", -1)[5]);
                long time = (nextApproveDate - firstApproveDate);
                //Difference time between first time and the next
                TimeUnit.MILLISECONDS.sleep(time);
                System.out.println("ArticleID " + next.split(",", -1)[1]);
                long timestamp = Long.parseLong(next.split(",", -1)[5]) * 1000;
                LocalDateTime triggerTime2 =
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                                ZoneOffset.UTC.normalized());
                System.out.println("date " + triggerTime2 + "\n");
                ProducerKafka.produce(producer, line,"comments");
            }
            line = next;
        }
        producer.close();

    }

    private static boolean checkTime(String line) {

        String[] str = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        try {

            if (Long.parseLong(str[5]) < 1514764800)
                return false;
        } catch (NumberFormatException num) {
            return false;
        }
        return true;

    }


}





