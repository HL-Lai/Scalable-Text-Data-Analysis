package text_analysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import com.vader.sentiment.analyzer.SentimentAnalyzer

object polarity {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master(master = "local[*]")
      .appName(name = "Polarity")
      .getOrCreate()

    val file_path = "src/main/scala/text_analysis/data_preprocessed.csv"

    val csv_options = Map("header" -> "true",
      "quote" -> "\"",
      "escape" -> "\"",
      "multiline" -> "true")

    val csvDF = spark.read
      .options(csv_options)
      .option("inferSchema", "true")
      .csv(path = file_path)
      .drop("_c0")

//    csvDF.show()
//    csvDF.printSchema()
    val csvRDD = csvDF.rdd

// ==========================================================================================

    val po_start_time = System.nanoTime()

    val polarityRdd = csvRDD.map(row => {
      val review_text = row.getString(row.fieldIndex("review_text"))
      val sentimentPolarities = SentimentAnalyzer.getScoresFor(review_text)
      Row.fromSeq(row.toSeq :+ sentimentPolarities.getCompoundPolarity.toDouble)
    })

    val po_run_time = (System.nanoTime() - po_start_time) / 1000000000.0
    println(s"Elapsed time for calculating polarity: $po_run_time seconds")

    //    polarityRdd.take(20).foreach(println)

    val schema = StructType(csvDF.schema.fields :+ StructField("Polarity", DoubleType, true))
    val polarityDF = spark.createDataFrame(polarityRdd, schema)

    polarityDF.show()

    val polarityRdd2 = polarityDF.rdd
    //    polarityRdd2.take(10).foreach(println)

// ==========================================================================================

    val wc_start_time = System.nanoTime()

    val wcRdd = polarityRdd2.map(row => {
      val review_text = row.getString(row.fieldIndex("review_text"))
      val word_count = review_text.split("\\s+").map(word => (word, 1)).groupBy(_._1).mapValues(_.map(_._2).sum)
      Row.fromSeq(row.toSeq :+ word_count.values.sum)
    })

    val wc_run_time = (System.nanoTime() - wc_start_time) / 1000000000.0
    println(s"Elapsed time for word count: $wc_run_time seconds")

//    wcRdd.take(10).foreach(println)

    val schema2 = StructType(schema :+ StructField("Word length", IntegerType, true))
    val wcDF = spark.createDataFrame(wcRdd, schema2)

    wcDF.show()

//    wcDF.write.csv("src/main/scala/text_analysis")

    spark.stop()
  }
}
