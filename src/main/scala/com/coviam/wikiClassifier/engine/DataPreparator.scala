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

class DataPreparator() extends PPreparator[TrainingData, PreparedData]{

  override def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {

    val obs = trainingData.contentAndcategory
    val sqlContext = SQLContext.getOrCreate(sc)
    val phraseDataframe = sqlContext.createDataFrame(obs).toDF("content", "category")
    val categories: Map[String,Int] = phraseDataframe.map(row => row.getAs[String]("category")).collect().zipWithIndex.toMap
    val tf = processPhrase(phraseDataframe)
    val labeledpoints = tf.map(row => new LabeledPoint(categories(row.getAs[String]("category")).toDouble, row.getAs[Vector]("rowFeatures")))
    PreparedData(trainingData,labeledpoints)
  }

  def processPhrase(phraseDataframe:DataFrame): DataFrame ={

    val tokenizer = new Tokenizer_new().setInputCol("content").setOutputCol("unigram")
    val unigram = tokenizer.transform(phraseDataframe)

    val remover = new StopWordsRemover().setInputCol("unigram").setOutputCol("filtered")
    val stopRemoveDF = remover.transform(unigram)

    var htf = new HashingTF().setInputCol("filtered").setOutputCol("rowFeatures")
    val tf = htf.transform(stopRemoveDF)

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
