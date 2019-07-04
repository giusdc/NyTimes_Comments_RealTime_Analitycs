package flink.utils.flink.query2;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Comparator;
import java.util.List;

public class Query2Result implements AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow> {

    private String file;
    private long lag;

    public Query2Result(String file,long lag) {
        this.file=file;
        this.lag=lag;

    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {
        if(this.file.equals("commentmonthly.csv")){
            System.out.println();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.file,true));
        writer.write(timeWindow.getStart()-lag+",");
        List<Tuple2<String,Integer>> list= Lists.newArrayList(iterable.iterator());
        //Sorting for having from count_h00 to count_h22
        list.sort(Comparator.comparing(x -> x.f0));
        for (int x=0;x<list.size();x++){
            writer.write(""+list.get(x).f1+","+"");
        }
        writer.write("\n");
        writer.close();
    }
}
