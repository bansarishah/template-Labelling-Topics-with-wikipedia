package com.coviam.wikiClassifier.engine

import org.apache.predictionio.controller.{Engine, EngineFactory, IEngineFactory}


object wikiClassifierEngine extends EngineFactory{

  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[DataPreparator],
      Map("Naivebayes" -> classOf[Algorithm]),
      classOf[Serving]
    )
  }
}