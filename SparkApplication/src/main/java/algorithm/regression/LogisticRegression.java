package algorithm.regression;
import algorithm.*;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class LogisticRegression extends SparkAlgorithmMeasure {

    public LogisticRegression(JavaSparkContext sc) {
        super(sc);
    }

    @Override
    protected void executeCore() {
        /*result = this.getTextFile()
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);*/
    }

    @Override
    protected void printResults() {

    }

    @Override
    public String datasetFileName() {
        return "adult.data";
    }

    @Override
    protected String dataSetFilePath() {
        return "src/main/resources/";
    }

    @Override
    protected void persistResults() {

    }
}
