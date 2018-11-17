package algorithm.machine_learning;

import algorithm.SparkAlgorithmMeasure;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public abstract class SparkMachineLearningAlgorithmMeasure extends SparkAlgorithmMeasure {
    SparkMachineLearningAlgorithmMeasure(JavaSparkContext sc, Integer nbIter) {
        super(sc,nbIter);
    }

    JavaRDD<LabeledPoint> getParsedData(){
        return this.getTextFile().map(s -> {
            String[] sarray = s.split(";");
            double[] values = new double[3];
            values[0] = DateTime.parse(sarray[0] + " " + sarray[1], DateTimeFormat.forPattern("dd/mm/yyyy HH:mm:ss")).getMillis();
            if (sarray[2].equals("?")) {
                   return new LabeledPoint(values[0],Vectors.dense(new double[0]));
            }
            values[1] = Double.parseDouble(sarray[2]);
            if (sarray[3].equals("?")) {
                   return new LabeledPoint(values[0],Vectors.dense(new double[0]));
            }
            values[2] = Double.parseDouble(sarray[3]);
            double[] features = {values[1], values[2]};
            return new LabeledPoint(values[0], Vectors.dense(features));
        }).filter(point -> point.features().size() > 0);        // gestion outliers

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
