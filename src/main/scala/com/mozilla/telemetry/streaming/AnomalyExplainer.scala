package com.mozilla.telemetry.streaming

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

import com.mozilla.telemetry.timeseries.SchemaBuilder

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.{ScallopConf, ScallopOption}

import edu.stanford.futuredata.macrobase.analysis.summary.IncrementalSummarizer
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType
import edu.stanford.futuredata.macrobase.datamodel.{Row, Schema, DataFrame => MBDataFrame}
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader


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
    .add[Float]("latency")
    .build

  private[streaming] def process(pings: DataFrame): DataFrame = {

    // TODO P1: classify and data points based on percentile cutoffs
    // TODO P3: exponentially damped resevoir sampling for training data
    // TODO P1: add sliding window summarizer that closely follows spark streaming windows
    val outlierSummarizer = new IncrementalSummarizer()
    //outlierSummarizer.setAttributes(attributes)

    pings
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    // TODO P2: decide on initial data source
    // TODO P2: add entrypoint for spark-streaming context
  }
}
