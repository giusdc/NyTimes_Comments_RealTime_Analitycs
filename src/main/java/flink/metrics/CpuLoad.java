package flink.metrics;


import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import scala.util.parsing.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;


public class CpuLoad {

    private static JsonArray JsonArrayjsonObject;

    public static void main(String[] args) throws Exception {

        BufferedWriter cpuWriter1;
        BufferedWriter cpuWriter2;
        BufferedWriter cpuWriter3;
        BufferedWriter thrFlinkWriter;
        BufferedWriter thrSourceFlinkWriter;
        BufferedWriter latencyWriter;
        BufferedWriter thrWriter;
        long start = System.currentTimeMillis();

        for (;;) {

            cpuWriter1= new BufferedWriter(
                    new FileWriter("cpu1.txt",true));
            cpuWriter2= new BufferedWriter(
                    new FileWriter("cpu2.txt",true));
            cpuWriter3= new BufferedWriter(
                    new FileWriter("cpu3.txt",true));
            thrFlinkWriter= new BufferedWriter(
                    new FileWriter("thrMapFlink.txt",true));
            thrSourceFlinkWriter= new BufferedWriter(
                    new FileWriter("thrSourceFlink.txt",true));
            /*
            latencyWriter= new BufferedWriter(
                    new FileWriter("latencykafka.txt",true));
            thrWriter= new BufferedWriter(
                    new FileWriter("throuhputkafka.txt",true));*/
            thrFlinkWriter.write(sendGet(args[0],3));
            thrSourceFlinkWriter.write(sendGet(args[1],3));
            cpuWriter1.write(sendGet(args[2],0));
            cpuWriter2.write(sendGet(args[3],0));
            cpuWriter3.write(sendGet(args[4],0));

            /*
            latencyWriter.write(sendGet(args[1],1));
            thrWriter.write(sendGet(args[1],2));*/

            cpuWriter1.close();
            cpuWriter2.close();
            cpuWriter3.close();
            thrFlinkWriter.close();
            thrSourceFlinkWriter.close();
            if(System.currentTimeMillis()-start>=600000)
                break;
            //latencyWriter.close();
            //thrWriter.close();
            System.out.print(".");
            TimeUnit.SECONDS.sleep(5);
        }

    }


    // Get country for each city
    public static String sendGet(String url, int i) throws IOException {

        //String url = "https://";

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        switch (i){
            //Cpu flink
            case 0:
                JsonArrayjsonObject = (JsonArray) new JsonParser().parse(response.toString());
                JsonObject ris = JsonArrayjsonObject.get(0).getAsJsonObject();
                return String.valueOf(ris.get("value")+"\n");
                //Latency Kafka
            case 1:
                JsonObject jsonObject = (JsonObject) new JsonParser().parse(response.toString());
                JsonObject valueObject =(JsonObject) jsonObject.get("value");
                try {
                    double latency = Double.parseDouble(String.valueOf(valueObject.get("process-latency-avg")));
                    return String.valueOf(latency/Math.pow(10,6)+"\n");
                }catch (Exception e){
                    return "";
                }


            case 2:
                JsonObject thrJson = (JsonObject) new JsonParser().parse(response.toString());
                JsonObject thrJsonObj =(JsonObject) thrJson.get("value");
                try {
                    return String.valueOf(thrJsonObj.get("process-rate")+"\n");
                }
                catch (Exception e){
                    return "";
                }
            case 3:
                JsonArray jsonArr = (JsonArray) new JsonParser().parse(response.toString());
                JsonObject result = jsonArr.get(0).getAsJsonObject();
                return String.valueOf(result.get("avg")+"\n");
                default:
                    return "";

        }




    }

}
