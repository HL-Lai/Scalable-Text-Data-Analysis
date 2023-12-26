package text_analysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


object preprocess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master(master = "local[1]")
      .appName(name = "GetData")
      .getOrCreate()

    val file_path = "src/main/scala/text_analysis/data_all.csv"

    val csv_options = Map("header" -> "true",
      "quote" -> "\"",
      "escape" -> "\"",
      "multiline" -> "true")

    val csvDF = spark.read
      .options(csv_options)
      .option("inferSchema", "true")
      .csv(path = file_path)
      .drop("_c0")
      .na.drop("any")

    println(s"(Original) no. of data: (${csvDF.count()})")
    csvDF.printSchema()
    val csvDF2 = csvDF.withColumn("vote", csvDF("vote").cast("Double"))
//    csvDF2.printSchema()

    val csvRDD = csvDF2.rdd

//    csvDF2.show()
//    csvRDD.take(5).foreach(println)

// ==========================================================================================

    val filteredRdd = csvRDD.filter(row => row != null)
    println(s"(Filtered) no. of data: (${filteredRdd.count()})")

    filteredRdd.take(10).foreach(println)

// ==========================================================================================

    val se_start_time = System.nanoTime()

    val sentimentRdd = filteredRdd.map(row => {
      val ratings = row.getDouble(row.fieldIndex("ratings"))
      val sentiment = ratings match {
        case 4.0 | 5.0 => "Positive"
        case 3.0 => "Neutral"
        case 1.0 | 2.0 => "Negative"
        case _ => "Unknown"
      }
      Row.fromSeq(row.toSeq :+ sentiment)
    })
    val se_totalTime = (System.nanoTime() - se_start_time) / 1000000000.0
    println(s"Elapsed time for getting sentiment: ${se_totalTime} seconds")

    val schema = StructType(csvDF2.schema.fields :+ StructField("sentiment", StringType, true))
    val sentimentDF = spark.createDataFrame(sentimentRdd, schema)
    val sentimentRdd2 = sentimentDF.rdd

//    sentimentDF.printSchema()
//    sentimentRdd2.take(10).foreach(println)

// ==========================================================================================

    val he_start_time = System.nanoTime()

    val helpfulnessRdd = sentimentRdd2.map(row => (row.getString(row.fieldIndex("asin")), row)).groupByKey()
      .flatMap {case (asin, rows) =>
        val voteSum = rows.map(_.getDouble(4)).sum
        rows.map(row => {
          val vote = row.getDouble(4)
          val helpfulnessRating = if (voteSum == 0) 0.0 else vote / voteSum
          Row.fromSeq(row.toSeq :+ helpfulnessRating)
        })
      }.filter(row => row.getDouble(9) > 0.1)

    val he_totalTime = (System.nanoTime() - he_start_time) / 1000000000.0
    println(s"Elapsed time for helpfulness rating: ${he_totalTime} seconds")

    val schema2 = StructType(sentimentDF.schema.fields :+ StructField("helpfulness rating", DoubleType, true))
    val helpfulnessDF = spark.createDataFrame(helpfulnessRdd, schema2)
    // helpfulnessDF.show()

    val helpfulnessRdd2 = helpfulnessDF.rdd
//    helpfulnessRdd2.take(10).foreach(println)

// ==========================================================================================

    val re_start_time = System.nanoTime()

    val cleanedRDD = helpfulnessRdd2.mapPartitions(partition => {
      partition.map(row => {
        val review_text = row.getString(5)
        val cleaned_text = review_text.replaceAll("\\[.*?\\]|http\\S+|\\d+", "").replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase()
        Row.fromSeq(row.toSeq.updated(5, cleaned_text))
      })
    })
    val re_totalTime = (System.nanoTime() - re_start_time) / 1000000000.0
    println(s"Elapsed time for text-punctuation cleaning: ${re_totalTime} seconds")

    //cleanedRDD.take(10).foreach(println)
    val cleanDF = spark.createDataFrame(cleanedRDD, schema2)

    //cleanDF.printSchema()
    cleanDF.show(20)

    // cleanDF.write.csv("src/main/scala/text_analysis/data_cleaned.csv")

    spark.stop()
  }

}
