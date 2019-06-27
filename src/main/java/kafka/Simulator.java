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
        Producer<String, String> producer = ProducerKafka.setConfig();
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
        if (checkLine(line))
            ProducerKafka.produce(producer, line);
        int count = 1;

        BufferedWriter writer = new BufferedWriter(new FileWriter("ciao.txt",true));
        //Produce on Kafka
        while ((next = reader.readLine()) != null) {
            count++;
            if (checkLine(next)){
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
                if(Long.parseLong(next.split(",", -1)[5])>=1515024000 && Long.parseLong(next.split(",", -1)[5])<=1515110399)
                {
                    writer.write(next+"\n");

                }
                ProducerKafka.produce(producer, next);
            }
            line = next;
        }
        producer.close();
        writer.close();

    }

    private static boolean checkLine(String line) {

        //1514821725,5a49915f7c459f246b63d661,809,25391091,userReply,1514820713,2,False,25389721,ChristineMcM,7,Unknown,RADF,3600063,"Milford, DE",,,,,,,,,,,,,,,,,,,
        String[] str = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        try {
            if (Long.parseLong(str[0]) < 1514764800)
                return false;
            if (Integer.parseInt(str[2]) < 0)
                return false;
            if (Integer.parseInt(str[3]) < 0)
                return false;
            if (!(str[4].equals("comment") || str[4].equals("userReply")))
                return false;
            if (Long.parseLong(str[5]) < 1514764800)
                return false;
            if (!(Integer.parseInt(str[6]) >= 1 && Integer.parseInt(str[6]) <= 3))
                return false;
            if (!(str[7].equals("False") || str[7].equals("True")))
                return false;
            if (Integer.parseInt(str[10]) < 0)
                return false;
            if(Integer.parseInt(str[13])<0)
                return false;
        } catch (NumberFormatException num) {
            return false;
        }
        return true;

    }


}





