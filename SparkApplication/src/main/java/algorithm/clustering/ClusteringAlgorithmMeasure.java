package algorithm.clustering;

import algorithm.SparkAlgorithmMeasure;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.Date;

public abstract class ClusteringAlgorithmMeasure extends SparkAlgorithmMeasure {
    ClusteringAlgorithmMeasure(JavaSparkContext sc, Integer nbIter) {
        super(sc,nbIter);
    }

    JavaRDD<Vector> getParsedData(){
        JavaRDD<Vector> parsedData = this.getTextFile().map(s -> {
            String[] sarray = s.split(";");
            double[] values = new double[3];
            values[0] = DateTime.parse(sarray[0] + " " + sarray[1], DateTimeFormat.forPattern("dd/mm/yyyy HH:mm:ss")).getMillis();
            if(sarray[2].equals("?")){
                return Vectors.dense(new double[0]);
            }
            values[1] = Double.parseDouble(sarray[2]);
            if(sarray[3].equals("?")){
                return Vectors.dense(new double[0]);
            }
            values[2] = Double.parseDouble(sarray[3]);
            return Vectors.dense(values);
        }).filter(vector -> vector.size() > 0);         // gestion des outliers
        return new StandardScaler(true,  true).fit(parsedData.rdd()).transform(parsedData);
    }
    @Override
    protected abstract void executeCore();

    @Override
    protected abstract void printResults() throws Exception;

    @Override
    protected String dataSetFilePath() {
        return "src/main/resources/";
    }

    @Override
    public String datasetFileName() {
        return "household_power_consumption_small.txt";
    }

    @Override
    protected abstract void persistResults() throws Exception;


}
