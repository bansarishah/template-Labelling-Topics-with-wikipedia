package com.coviam.wikiClassifier.engine

import org.apache.predictionio.controller.PPreparator
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{Tokenizer, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.callUDF

/**
  * Created by bansarishah on 8/20/16.
  */
class DataPreparator() extends PPreparator[TrainingData, PreparedData]{

  override def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {

    val obs = trainingData.contentAndcategory
    val sqlContext = SQLContext.getOrCreate(sc)
    val phraseDataframe = sqlContext.createDataFrame(obs).toDF("content", "category")
    phraseDataframe.show(2)
    val categories: Map[String,Int] = phraseDataframe.map(row => row.getAs[String]("category")).collect().zipWithIndex.toMap
    categories.take(3).foreach(x => println(x._1,x._2))
    val tf = processPhrase(phraseDataframe)
    tf.show(3)
    val labeledpoints = tf.map(row => new LabeledPoint(categories(row.getAs[String]("category")).toDouble, row.getAs[Vector]("rowFeatures")))
    PreparedData(trainingData,labeledpoints)
  }

  def processPhrase(phraseDataframe:DataFrame): DataFrame ={

    val tokenizer = new Tokenizer_new().setInputCol("content").setOutputCol("unigram")
    val unigram = tokenizer.transform(phraseDataframe)
    unigram.select("unigram").show(3)

    val remover = new StopWordsRemover().setInputCol("unigram").setOutputCol("filtered")
    val stopRemoveDF = remover.transform(unigram)
    stopRemoveDF.select("filtered").show(3)

    var htf = new HashingTF().setInputCol("filtered").setOutputCol("rowFeatures")
    val tf = htf.transform(stopRemoveDF)
    tf.select("rowFeatures").show(3)

    tf
  }
}

case class PreparedData(var trainingData: TrainingData, var labeledpoints:RDD[LabeledPoint]) extends Serializable{
}

class Tokenizer_new extends Tokenizer(){

  override def createTransformFunc: (String) => Seq[String] = { str =>
    val unigram = str.replaceAll("[.*|!*|?*|=*|)|(]","").replaceAll("((www\\.[^\\s]+)|(https?://[^\\s]+)|(http?://[^\\s]+))","")
      .replaceAll("(0-9*)|(0-9)+(A-Za-z)*(.*|:*)","").toLowerCase().split("\\s+").toSeq
    unigram
  }
}
