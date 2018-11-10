package algorithm;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Interface commune des mesures d'algorithmes Spark
 */
public abstract class SparkAlgorithmMeasure {
    /** Le RDD du fichier */
    private JavaRDD<String> textFile;
    public static final Integer NB_ITER = 10000;

    public SparkAlgorithmMeasure(JavaSparkContext sc){
        this.textFile =  sc.textFile(datasetFileName());
    }

    /**
     * Exécution de l'algorithme
     * @return la durée d'exécution moyenne en ms sur NB_ITER itérations
     */
    public Long execute() throws Exception {
        long startTime = System.currentTimeMillis();
        for(int i = 0;i<NB_ITER;i++) {
            executeCore();
       //     printResults();
            //  persistResults();
        }
        long stopTime = System.currentTimeMillis();
        return (stopTime - startTime) / NB_ITER;
    }

    protected abstract void executeCore();
    /** Affichage des résultats */
    protected abstract void printResults() throws Exception;
    /** Le dataset utilisé par l'algorithme*/
    public abstract String datasetFileName();

    public JavaRDD<String> getTextFile() {
        return textFile;
    }

    protected abstract void persistResults() throws Exception;
}
