package Training

import com.github.mrpowers.spark.stringmetric.SimilarityFunctions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Dataframe extends App {

  val spark = SparkSession.builder().master("local").getOrCreate()
  val path1 = this.getClass.getClassLoader.getResource("data/train.csv").getFile
  val path2 = this.getClass.getClassLoader.getResource("data/train2.csv").getFile


  val df1 = spark.read.option("header", "true").csv(path1).withColumn("full_name",
    concat(col("first_name"), lit(" "),
      col("middle_name"), lit(" "), col("last_name")))

  val df2 = spark.read.option("header", "true").csv(path2).withColumn("full_name",
    concat(col("first_name"), lit(" "),
      col("middle_name"), lit(" "), col("last_name")))

  //Levenstein Score
  df1.crossJoin(df2)
    .withColumn("levenstein_score", levenshtein(df1("full_name"), df2("full_name")))
    .show(truncate = false)

  // Cosine_Distance
  df1.crossJoin(df2)
    .withColumn("cosine_distance", cosine_distance(df1("full_name"), df2("full_name")))
    .show(truncate = false)

  //Fuzzy_Score
  df1.crossJoin(df2)
    .withColumn("fuzzy_score", fuzzy_score(df1("full_name"), df2("full_name")))
    .show(truncate = false)

  //Jaccard_Similarity
  df1.crossJoin(df2)
    .withColumn("jaccard_score", jaccard_similarity(df1("full_name"), df2("full_name")))
    .show(truncate = false)

  //Jaro_Winkler
  df1.crossJoin(df2)
    .withColumn("jaro_score", jaro_winkler(df1("full_name"), df2("full_name")))
    .show(truncate = false)
}
