package flink.metrics;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;


public class CpuLoad {

    public static void main(String[] args) throws Exception {

        BufferedWriter writer = new BufferedWriter(
                new FileWriter("cpu.txt",true));


        for (int i = 0; i <800 ; i++) {
            TimeUnit.MILLISECONDS.sleep(50);
            writer.write(sendGet(args[0])+"\n");

        }
    }


    // Get country for each city
    public static String sendGet(String url) throws Exception {

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

        JsonObject jsonObject = (JsonObject) new JsonParser().parse(String.valueOf(response));
        String value =  String.valueOf(jsonObject.get("value"));
        return value;


    }

}
