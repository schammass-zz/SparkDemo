package Training

import com.github.mrpowers.spark.stringmetric.SimilarityFunctions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object ComparingDataFrames extends App {

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

    def levensteinScore(x: DataFrame, y: DataFrame): DataFrame = {
    df1.crossJoin(df2)
      .withColumn("levenstein_score", levenshtein(df1("full_name"), df2("full_name")))
  }

  levensteinScore(df1, df2).show()

  // Cosine_Distance

  def cosineDistance(x: DataFrame, y: DataFrame): DataFrame = {
    df1.crossJoin(df2)
      .withColumn("cosine_distance", cosine_distance(df1("full_name"), df2("full_name")))
  }

  cosineDistance(df1, df2).show()

  //Fuzzy_Score

  def fuzzyScore(x: DataFrame, y: DataFrame): DataFrame = {
    df1.crossJoin(df2)
      .withColumn("fuzzy_score", fuzzy_score(df1("full_name"), df2("full_name")))
  }

  fuzzyScore(df1, df2).show()

  //Jaccard_Similarity
  def jaccardSimilarity(x: DataFrame, y: DataFrame): DataFrame = {

    df1.crossJoin(df2)
      .withColumn("jaccard_score", jaccard_similarity(df1("full_name"), df2("full_name")))
  }

  jaccardSimilarity(df1, df2).show()


  //Jaro_Winkler

  def jaroWinkler(x: DataFrame, y: DataFrame): DataFrame = {
    df1.crossJoin(df2)
      .withColumn("jaro_score", jaro_winkler(df1("full_name"), df2("full_name")))
  }

  jaroWinkler(df1, df2).show()
}