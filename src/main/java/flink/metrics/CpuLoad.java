package flink.metrics;


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import scala.util.parsing.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;


public class CpuLoad {

    private static JsonArray JsonArrayjsonObject;

    public static void main(String[] args) throws Exception {

        BufferedWriter writer;

        for (int i = 0; i <60; i++) {

            writer= new BufferedWriter(
                    new FileWriter("cpu.txt",true));

            TimeUnit.SECONDS.sleep(10);
            writer.write(sendGet(args[0])+"\n");
            writer.close();
            System.out.println(".");
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
        JsonArrayjsonObject = (JsonArray) new JsonParser().parse(response.toString());
        JsonObject ris = JsonArrayjsonObject.get(0).getAsJsonObject();
        return String.valueOf(ris.get("value"));



    }

}
