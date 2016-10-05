package com.coviam.wikiClassifier.engine


import org.apache.predictionio.controller._
import org.apache.predictionio.data.storage.{Event, PropertyMap, Storage}
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


class DataSource(val dsp: DataSourceParam) extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, EmptyActualResult]{

  def readEvent(sc:SparkContext) : RDD[WikiPage] = {
    val eventDB = Storage.getPEvents()
    val eventRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("wiki_page"),
      eventNames = Some(List("train"))
      )(sc)
    val sentimentRDD : RDD[WikiPage] = eventRDD.map{
      event  =>
        val text = event.properties.get[String]("content")
        val category = event.properties.get[String]("category")
        WikiPage(text,category)
    }
    sentimentRDD
  }

   override def readTraining(sc: SparkContext): TrainingData = {
    new TrainingData(readEvent(sc))
  }
}

class TrainingData(val contentAndcategory:RDD[WikiPage]) extends Serializable with SanityCheck{

  def sanityCheck(): Unit = {
    try {
      val obs = contentAndcategory.takeSample(false, 2)
      println("total observation",obs.length)
      (0 until obs.length).foreach(
        k => println("Observation " + (k + 1) + " label: " + obs(k))
      )
      println()
    } catch {
      case (e: ArrayIndexOutOfBoundsException) => {
        println()
        println("Data set is empty, make sure event fields match imported data.")
        println()
      }
    }
  }
}
case class Query(topics: Seq[Array[String]]) extends Serializable
case class DataSourceParam(appName:String, evalK:Int) extends Params

case class WikiPage(content:String, category:String)

