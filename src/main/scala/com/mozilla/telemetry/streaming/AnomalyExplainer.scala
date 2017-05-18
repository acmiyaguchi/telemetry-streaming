package com.mozilla.telemetry.streaming

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import com.mozilla.telemetry.timeseries.SchemaBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, functions => F}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import edu.stanford.futuredata.macrobase.analysis.summary.IncrementalSummarizer
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType
import edu.stanford.futuredata.macrobase.datamodel.{Row, Schema, DataFrame => MBDataFrame}
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, State, StreamingContext}


// MacroBase DataFrames only handle attributes and metrics, which are string and doubles respectively. Conversion
// is simply collecting the rdds onto the driver.
class SparkDataFrameLoader(df: DataFrame) extends DataFrameLoader {
  val sparkDF: DataFrame = df
  var columnTypes: mutable.Map[String, Schema.ColType] = mutable.Map()

  override def setColumnTypes(types: util.Map[String, ColType]): DataFrameLoader = {
    columnTypes = types.asScala
    return this
  }

  override def load(): MBDataFrame = {
    var schema = new Schema()

    for(name <- sparkDF.columns) {
      schema.addColumn(columnTypes.getOrElse(name, Schema.ColType.STRING), name)
    }

    // TODO P2: handle bad data-types
    // naively assume that incoming data is composed of types {STRING, DOUBLE}
    val rows = sparkDF.collect()
      .map(row => new Row(row.toSeq.asInstanceOf[Seq[Object]].asJava))
      .toSeq.asJava

    new MBDataFrame(schema, rows)
  }
}


object AnomalyExplainer {
  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val inputPath: ScallopOption[String] = opt[String](
      "inputPath",
      descr = "path to dataset using path styled access in spark",
      required = false)
    verify()
  }

  private val dimensionsSchema = new SchemaBuilder()
    .add[String]("client_id")
    .add[String]("env_build_id")
    .add[String]("env_build_version")
    .add[String]("os")
    .add[String]("country")
    .add[String]("channel")
    .build

  private val metricsSchema = new SchemaBuilder()
    .add[Double]("latency")
    .build

  private[streaming] def process(spark: SparkSession, pings: DataFrame): DataFrame = {
    /*
    This implements a crude equivalent of reservoir sampling. The reservoir size is k, and the item replacement weight
    is w. The weight in it's simplest form is the number of items seen so far. Items in the reserve are replaced at with
    rate p = k/w if |reservoir| = k. This will probably require modification to implement bias toward more recent items.

    RDDs are cached in memory by setting ssc.remember to a value roughly 2x larger than the batch interval.
    The transform operation enables arbitrary rdd operations within the streaming context [2], which is
    used to join the cached rdd with the mini-batch.

    Another implementation could be done using mapWithState instead, which could implement a proper data-structure
    for sampling data across time windows. This approach seems suited aggregating across different attributes, sort
    if like a groupByKey but for streaming data. However, this might be inefficient since state is persisted by key. Since
    there is only a key per metric, this may not be the best way to maintain the data, but I still need to figure
    out the merits and disadvantage of this.

    Alternatively, it may not be worth the fuss if the size of reservoir is small. 20k samples works out to be 160kb,
    which is not a very expensive overhead at all.

    TODO P3: exponentially damped reservoir sampling for training data

    [1] http://stackoverflow.com/questions/30048684/spark-streaming-data-sharing-between-batches
    [2] https://spark.apache.org/docs/latest/streaming-programming-guide.html#transform-operation
    */
    var classifierReservoir: RDD[Double] = spark.sparkContext.emptyRDD
    var currentWeight = 0
    val reservoirSize = 20000

    pings.select("latency").transform { ds =>
      // Iterating over both RDDs is probably slow
      val batchSize = ds.count()
      val currentSize = classifierReservoir.count()
      val incoming = ds.rdd.map(_.getAs[Double]("latency"))
      currentWeight += batchSize

      // Amount necessary to fill the reserve, and the amount that needs to be accounted for
      val topoff = reservoirSize - currentSize
      val spillover = batchSize - topoff

      if (spillover <= 0) {
        classifierReservoir = classifierReservoir ++ incoming
      }
      else {
        // Number of items remove to account for spillover
        val replacementCount = (1D * reservoirSize / currentWeight) * spillover
        val probReserve = (currentSize - replacementCount) / currentSize
        val probIncoming = (topoff + replacementCount) / batchSize

        classifierReservoir = classifierReservoir.sample(false, probReserve) ++ incoming.sample(false, probIncoming)
      }
      pings
    }

    val median = spark
      .createDataset(classifierReservoir)
      .toDF("latency")
      .stat.approxQuantile("latency", Array(0.5), 0.01)(0)
    val MAD = pings
      .select(F.abs(F.col("latency")-F.lit(median)).alias("residual"))
      .stat.approxQuantile("residual", Array(0.5), 0.01)(0)

    // score pings using a modified Z-score using median and MAD instead of mean and standard deviation
    val scoredPings = pings.withColumn("score", (F.col("latency")-F.lit(median))/F.lit(MAD))


    // TODO P1: classify and data points based on percentile cutoffs
    // TODO P1: add sliding window summarizer that closely follows spark streaming windows
    val outlierSummarizer = new IncrementalSummarizer()
    //outlierSummarizer.setAttributes(attributes)

    pings
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("MacroBase")
      .master("local[*]")
      .getOrCreate()

    // TODO: choose a real interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(30))
    ssc.remember(Seconds(60))

    val schema = SchemaBuilder.merge(dimensionsSchema, metricsSchema)
    val path = "s3://net-mozaws-prod-us-west-2-pipeline-analysis/amiyaguchi/macrobase/client-latency"
    val inputDF = spark
      .readStream
      .schema(schema)
      .parquet(path)

    process(spark, inputDF)
  }
}
