package algorithm;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Interface commune des mesures d'algorithmes Spark
 */
public abstract class SparkAlgorithmMeasure {
    /** Le RDD du fichier */
    private JavaRDD<String> textFile;
    public static Integer NB_ITER = 50;
    protected  JavaSparkContext jsc;
    public SparkAlgorithmMeasure(JavaSparkContext sc){
        this.jsc = sc;
        this.textFile =  sc.textFile(datasetFileName());
    }

    /**
     * Exécution de l'algorithme
     * @return la durée d'exécution moyenne en ms sur NB_ITER itérations
     */
    public Long execute(double n) throws Exception {
        long startTime = System.currentTimeMillis();
        System.out.println("Start = " + startTime);
        for(int i = 0;i<NB_ITER;i++) {
            executeCore(n);
           // printResults();
            //  persistResults();
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("Stop = " + stopTime);
        return (stopTime - startTime) / NB_ITER;
    }

    protected abstract void executeCore(double n);
    /** Affichage des résultats */
    protected abstract void printResults() throws Exception;
    /** Le dataset utilisé par l'algorithme*/
    public abstract String datasetFileName();

    public JavaRDD<String> getTextFile() {
        return textFile;
    }

    protected abstract void persistResults() throws Exception;
}
