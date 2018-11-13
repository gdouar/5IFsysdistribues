import algorithm.SparkAlgorithmMeasure;
import algorithm.clustering.KMeansClusteringMeasure;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class Main {

    public static void main(String[] args){

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");                // limitation du niveau de log
        ArrayList<Pair<String,Pair<Integer, Long>>> arrayExec = new ArrayList<>();
        String files[] = {"household_power_consumption_VerySmall.txt" , "household_power_consumption_small.txt" , "household_power_consumption_medium.txt",
                             "household_power_consumption_big.txt" /*, "household_power_consumption_very_big.txt" */};
        for(int j=0;j<files.length;j++) {
            for (int i = 1; i <= 10; i++) {
                Double percentage = (double) i / 10;
                SparkAlgorithmMeasure algo = new KMeansClusteringMeasure(sc, 1000, 10, 10, files[j], percentage);
                try {
                    Long executionTimeMs = algo.execute();
                    arrayExec.add(new MutablePair<>(files[j], new MutablePair<>(i * 10, executionTimeMs)));
                    System.out.println("Average time for " + (double) i * 10 + "% of dataset " + files[j] + ": " + executionTimeMs + " ms.");
                } catch (Exception ex) {
                    System.err.println(ex.getMessage());
                }
            }
        }
        // save performance results
        HashMap<String, String> fileContents = new HashMap<>();
        for(int j=0;j<arrayExec.size();j++) {
            String toSave="";
            Pair<Integer, Long> pair = arrayExec.get(j).getValue();
            toSave += pair.getKey() + "," + pair.getValue();
            toSave += "\n";
            if(fileContents.containsKey(arrayExec.get(j).getKey())){
                fileContents.put(arrayExec.get(j).getKey(), fileContents.get(arrayExec.get(j).getKey()) + toSave);
            }
            else {
                fileContents.put(arrayExec.get(j).getKey(), toSave);
            }
        }
        for(String file : fileContents.keySet()){
            try {
                PrintWriter out = new PrintWriter("results-" + file + ".csv");
                out.print(fileContents.get(file));
                out.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

        }
        }

}
