package Training

import com.github.mrpowers.spark.stringmetric.SimilarityFunctions._
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


object ComparingDataFrames extends App {

  val spark = SparkSession.builder().master("local").getOrCreate()
  val path1 = this.getClass.getClassLoader.getResource("data/train.csv").getFile
  val path2 = this.getClass.getClassLoader.getResource("data/train2.csv").getFile


  val df1 = spark.read.option("header", "true").csv(path1).withColumn("full_name1",
    concat(col("first_name1"), lit(" "),
      col("middle_name1"), lit(" "), col("last_name1")))

  val df2 = spark.read.option("header", "true").csv(path2).withColumn("full_name2",
    concat(col("first_name2"), lit(" "),
      col("middle_name2"), lit(" "), col("last_name2")))

  //Levenstein Score

    def levenshteinScore(x: DataFrame, y: DataFrame): DataFrame = {
    df1.crossJoin(df2)
      .withColumn("levenshtein_score", levenshtein(df1("full_name1"), df2("full_name2")))
  }

  val levenshteinDF = levenshteinScore(df1, df2)
  //levenshteinDF.show()

  //val full_name2_lenght2_Array =

  //levenshteinDF
   //.withColumn("levenshtein_score_pct", lit(1)
     // - col("levenshtein_score") / greatest(length(col("full_name1")),length(col("full_name2"))))

  val levenshteinDF2 = levenshteinDF
    .withColumn("full_name1_length", length(col("full_name1")))
    .withColumn("full_name2_length", length(col("full_name2")))
    .withColumn("full_name_length_max", when(col("full_name1_length") >= col("full_name2_length"), col("full_name1_length")).otherwise(col("full_name2_length")))
    .withColumn("levenshtein_score_error_pct", col("levenshtein_score") / col("full_name_length_max"))
    .withColumn("levenshtein_score_pct", (lit(1) - col("levenshtein_score_error_pct")))
    .drop("first_name1","middle_name1","last_name1","birth_date1","gender1", "first_name2","middle_name2",
      "last_name2","birth_date2","gender2","full_name1_length", "full_name2_length", "full_name_length_max",
      "levenshtein_score_error_pct")
    .where(col("levenshtein_score_pct") > 0.8)

  levenshteinDF2.show(48,false)


  /* Cosine_Distance

  def cosineDistance(x: DataFrame, y: DataFrame): DataFrame = {
    df1.crossJoin(df2)
      .withColumn("cosine_distance", cosine_distance(df1("full_name"), df2("full_name")))
  }

  cosineDistance(df1, df2).show() */

  //Fuzzy_Score

  /* def fuzzyScore(x: DataFrame, y: DataFrame): DataFrame = {
    df1.crossJoin(df2)
      .withColumn("fuzzy_score", fuzzy_score(df1("full_name"), df2("full_name")))
  }

  fuzzyScore(df1, df2).show() */

  //Jaccard_Similarity
  def jaccardSimilarity(x: DataFrame, y: DataFrame): DataFrame = {

    df1.crossJoin(df2)
      .withColumn("jaccard_score", jaccard_similarity(df1("full_name1"), df2("full_name2")))
  }

  jaccardSimilarity(df1, df2).show()


  //Jaro_Winkler

  /*def jaroWinkler(x: DataFrame, y: DataFrame): DataFrame = {
    df1.crossJoin(df2)
      .withColumn("jaro_score", jaro_winkler(df1("full_name"), df2("full_name")))
  }

  jaroWinkler(df1, df2).show()*/
}