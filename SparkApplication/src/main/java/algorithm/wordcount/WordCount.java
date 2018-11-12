package algorithm.wordcount;
import algorithm.*;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
/** Évaluation d'une fonctionnalité basique Spark*/
public class WordCount extends SparkAlgorithmMeasure {

    public WordCount(JavaSparkContext sc) {
        super(sc);
    }
    /** Le résultat du comptage */
    private JavaPairRDD<String, Integer> result;

    @Override
    protected void executeCore() {
        result = this.getTextFile()
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
    }

    @Override
    protected void printResults() throws Exception {
        if(result != null) {
            result.foreach(p -> System.out.println(p));
            System.out.println("Total words: " + result.count());
        }
        else throw new Exception("Must execute() algorithm first !");
    }

    @Override
    public String datasetFileName() {
        return "shakespeare.txt";
    }

    @Override
    protected String dataSetFilePath() {
        return "src/main/resources/";
    }

    @Override
    protected void persistResults() throws Exception {
        if(result != null) {
            String fileName = "tmp/shakespeareWordCount";
            File file = new File(fileName);
            if (file.exists()) {
                try {
                    FileUtils.deleteDirectory(file);
                } catch (IOException ex) {
                    System.err.println("Could not delete dir :" + fileName);
                }
            }
            result.saveAsTextFile(fileName);
        }
        else throw new Exception("Must execute() algorithm first !");

    }
}
