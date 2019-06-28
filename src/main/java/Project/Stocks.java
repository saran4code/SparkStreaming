package Project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.text.SimpleDateFormat;

public class Stocks implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	
	public static void main(String[] args) throws InterruptedException {		
		System.setProperty("hadoop.home.dir", "/home/saran/Desktop/");
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStock");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		jssc.checkpoint("checkpoint_dir");
		Logger.getRootLogger().setLevel(Level.ERROR);
		System.setProperty("hadoop.home.dir", "/home/saran/Desktop/");
		JavaDStream<String> json= jssc.textFileStream("/home/saran/Desktop/Technical_notes/BDE_Notes/C5_M6_Spark_Streaming/Case_Study/Source");
		System.out.println("json");
		json.print();
		
		JavaDStream<Map<String, StockPrice>> stockStream = 
				json.map(x -> {
			ObjectMapper mapper = new ObjectMapper();
			SimpleDateFormat df = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
			mapper.setDateFormat(df);
			
			TypeReference<List<StockPrice>> mapType = new TypeReference<List<StockPrice>>() { };
			List<StockPrice> list = mapper.readValue(x, mapType);
			Map<String, StockPrice> map = new HashMap<>();
			for (StockPrice sp : list) {
				map.put(sp.getSymbol(), sp);
				}
			return map;
			});
		
		System.out.println("stockStream");		
		JavaPairDStream<String, AverageTuple> windowStockDStream = getWindowDStream(stockStream);
		
		JavaPairDStream<AverageTuple, String> swappedwindowStockDStream = windowStockDStream.mapToPair(x -> x.swap());
		JavaPairDStream<AverageTuple, String> sorted = swappedwindowStockDStream.transformToPair(
				new Function<JavaPairRDD<AverageTuple, String>, JavaPairRDD<AverageTuple, String>>() {
					
					private static final long serialVersionUID = 1L;
					public JavaPairRDD<AverageTuple, String> call(JavaPairRDD<AverageTuple, String> in) throws Exception {
			
						return in.sortByKey(new ProfitComparator());
			
				}
				});
		
		    sorted.print();
		    
		    sorted.foreachRDD(new VoidFunction<JavaPairRDD<AverageTuple, String>>() {
		    	
		    	private static final long serialVersionUID = 6767679;
		    	public void call(JavaPairRDD< AverageTuple,String> t)
		    	throws Exception {
		    		t.coalesce(1).saveAsTextFile("/home/saran/Desktop/Technical_notes/BDE_Notes/C5_M6_Spark_Streaming/Case_Study/Output" + java.io.File.separator + System.currentTimeMillis());
		    	}
		    });
		   
		   jssc.start();
		   jssc.awaitTermination();
		   
	}
	
	/*
	 * Here, SUM_REDUCER_AVG_DATA and DIFF_REDUCER_PRICE_DATA are lambda expressions
	 */
	
	private static Function2<AverageTuple, AverageTuple, AverageTuple> SUM_REDUCER_AVG_DATA = (a, b) -> {
		AverageTuple avg = new AverageTuple();
		avg.setCount(a.getCount() + b.getCount());
		avg.setAverageClose(a.getAverageClose() + b.getAverageClose());
		avg.setAverageOpen(a.getAverageOpen() + b.getAverageOpen());
		avg.setMaxprofit((a.getAverageClose() + b.getAverageClose()), (a.getAverageOpen() + b.getAverageOpen()), (a.getCount() + b.getCount()));
		return avg;
		
	};
	
	private static Function2<AverageTuple, AverageTuple, AverageTuple> DIFF_REDUCER_PRICE_DATA = (a, b) -> {
		AverageTuple avg = new AverageTuple();
		avg.setCount(a.getCount() - b.getCount());
		avg.setAverageClose(a.getAverageClose() - b.getAverageClose());
		avg.setAverageOpen(a.getAverageOpen() - b.getAverageOpen());
		avg.setMaxprofit((a.getAverageClose() - b.getAverageClose()), (a.getAverageOpen() - b.getAverageOpen()), (a.getCount() - b.getCount()));
		return avg;
		
	};
	
	
	private static JavaPairDStream<String, AverageTuple> getWindowDStream(
			JavaDStream<Map<String, StockPrice>> stockStream) {
		
		JavaPairDStream<String, AverageTuple> stockPriceStream 
				= getAvgDStream(stockStream, "MSFT");
		JavaPairDStream<String, AverageTuple> stockPriceGoogleStream = 
			getAvgDStream(stockStream, "GOOGL");
		JavaPairDStream<String, AverageTuple> stockPriceADBEStream = 
			getAvgDStream(stockStream, "ADBE");
		JavaPairDStream<String, AverageTuple> stockPriceFBStream = 
			getAvgDStream(stockStream, "FB");
		
		
		JavaPairDStream<String, AverageTuple> windowsMSFTDStream =
		stockPriceStream.reduceByKeyAndWindow(SUM_REDUCER_AVG_DATA,
				DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
				Durations.minutes(5));
		JavaPairDStream<String, AverageTuple> windowGoogDStream =
				stockPriceGoogleStream.reduceByKeyAndWindow(SUM_REDUCER_AVG_DATA,
						DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
						Durations.minutes(5));
		JavaPairDStream<String, AverageTuple> windowAdbDStream =
				stockPriceADBEStream.reduceByKeyAndWindow(SUM_REDUCER_AVG_DATA,
						DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
						Durations.minutes(5));
		JavaPairDStream<String, AverageTuple> windowFBDStream =
				stockPriceFBStream.reduceByKeyAndWindow(SUM_REDUCER_AVG_DATA,
						DIFF_REDUCER_PRICE_DATA, Durations.minutes(10),
						Durations.minutes(5));
		
		windowsMSFTDStream = windowsMSFTDStream.union(windowGoogDStream).union(windowAdbDStream).union(windowFBDStream);
		return windowsMSFTDStream;
		
	}
	
	private static JavaPairDStream<String, AverageTuple> getAvgDStream(JavaDStream<Map<String, StockPrice>> stockStream,
			String symbol) {
		JavaPairDStream<String, AverageTuple> stockAvgStream = stockStream
				.mapToPair(new PairFunction<Map<String, StockPrice>,String, AverageTuple>(){
					
					private static final long serialVersionUID = 1L;
					
					public Tuple2<String, AverageTuple>
					call(Map<String, StockPrice> map) throws Exception {
						if (map.containsKey(symbol)) {
							return new Tuple2<String,AverageTuple> (symbol,
									new AverageTuple(1, map.get(symbol).getPriceData().getOpen(),
											map.get(symbol).getPriceData().getClose(),(map.get(symbol).getPriceData().getClose()+map.get(symbol).getPriceData().getOpen()
													)));
						} else {
							return new Tuple2<String, 
									AverageTuple>(symbol, new AverageTuple());
							
						}
					}
					
				});
		return stockAvgStream;
		}
	

	}


