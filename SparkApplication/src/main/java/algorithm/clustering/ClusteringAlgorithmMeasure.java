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
    public ClusteringAlgorithmMeasure(JavaSparkContext sc, Integer nbIter) {
        super(sc,nbIter);
    }

    protected JavaRDD<Vector> getParsedData(){
        JavaRDD<Vector> parsedData = this.getTextFile().map(s -> {
            String[] sarray = s.split(";");
            double[] values = new double[sarray.length];
            values[0] = new Date(sarray[0]).toInstant().toEpochMilli();
            values[1] = DateTime.parse(sarray[1], DateTimeFormat.forPattern("HH:mm:ss")).getMillisOfDay();
            for(int i=2;i<sarray.length;i++) {
                if(sarray[i].equals("?")){
                    return Vectors.dense(new double[0]);
                }
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        }).filter(vector -> vector.size() > 0);         // gestion des outliers

        return new StandardScaler(true,  true).fit(parsedData.rdd()).transform(parsedData);
    }
    @Override
    protected abstract void executeCore(double n);

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
