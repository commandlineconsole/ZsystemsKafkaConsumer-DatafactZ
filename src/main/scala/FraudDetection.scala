package com.kal.datafactz
/**
  * Decisiontree model.
  */
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import java.io._
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree

import org.apache.log4j.{Level, Logger}

object FraudDetection {

  case class Fraud(imp: String, potential: String, m_id: String, auth: String, age: Int, Time: String, gender: String, name: String, pcode: String, cid: Int, m_cat: Int, amount: Double, marital: Double, edu: Double, children: Double, cardpan: Double, res: Int)

  case class History(label: Double, prediction: Double, importance: Double, potentiality: Double, customerid: Double, martial1: Double, mid: Double, cardpanno: Double, gender1: Double, pcode1: Double, amount1: Double, time1: Double)

  case class uspostal(pcode: String, State: String, Statecode: String)

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

//SQLContext 
    val conf = new SparkConf()
      .setAppName("SparkFraud")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._


    // function to parse input into Fraud class
    def parseFraud(str: String): Fraud = {
      val line = str.split(",")
      Fraud(line(0), line(1), line(2), line(3), line(4).toInt, line(5).substring(0,19).replace("T"," "), line(6), line(7), line(8), line(9).toInt, line(10).toInt, line(11).toDouble, line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble, line(16).toInt)
    }

    /* -------------------------------- MLLIB------------------------------------------ */
    //Creating and RDD used for training the model
   val textRDD = sc.textFile("file:///root/IBMzSystems/IBMdataSuper.csv")
   val FraudsRDD = textRDD.map(parseFraud).cache()
   val FraudsDF = FraudsRDD.toDF()
   FraudsDF.show()
	
	var midMap: Map[String, Int] = Map()
    var index: Int = 0
    FraudsRDD.map(Fraud => Fraud.m_id).distinct.collect.foreach(x => {
      midMap += (x -> index);
      index += 1
    })

    var genderMap: Map[String, Int] = Map()
    var index1: Int = 0
    FraudsRDD.map(Fraud => Fraud.gender).distinct.collect.foreach(x => {
      genderMap += (x -> index1);
      index1 += 1
    })

    var pcodeMap: Map[String, Int] = Map()
    var index2: Int = 0
    FraudsRDD.map(Fraud => Fraud.pcode).distinct.collect.foreach(x => {
      pcodeMap += (x -> index2);
      index2 += 1
    })

    var timeMap: Map[String, Int] = Map()
    var index3: Int = 0
    FraudsRDD.map(Fraud => Fraud.Time).distinct.collect.foreach(x => {
      timeMap += (x -> index3);
      index3 += 1
    })

    //- Defining the features array
    val mlprep = FraudsRDD.map(Fraud => {
      val importance = Fraud.imp.toInt - 1 // category
      val potentiality = Fraud.potential.toInt - 1 // category
      val customerid = Fraud.cid.toInt
      val martial1 = Fraud.marital.toInt
      val mid = midMap(Fraud.m_id) // category
      val cardpanno = Fraud.cardpan.toDouble
      val gender1 = genderMap(Fraud.gender) // category
      val pcode1 = pcodeMap(Fraud.pcode) // category
      val amount1 = Fraud.amount.toDouble
      val time1 = timeMap(Fraud.Time)
      val fraudLabel = Fraud.res.toInt
      Array(fraudLabel.toDouble, importance.toDouble, potentiality.toDouble, customerid.toDouble, martial1.toDouble, mid.toDouble, cardpanno.toDouble, gender1.toDouble, pcode1.toDouble, amount1.toDouble, time1.toDouble)
    })
    //Making LabeledPoint of features - this is the training data for the model

    val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10))))

    //my streming data with fraud flag
    val mymodeldata = mldata.toDF()

    val mldata0 = mldata.filter(x => x.label == 0).randomSplit(Array(0.85, 0.15))(1)
    val mldata1 = mldata.filter(x => x.label != 0)
    val mldata2 = mldata0 ++ mldata1
    val splits = mldata2.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))


    var categoricalFeaturesInfo = Map[Int, Int]()
    categoricalFeaturesInfo += (0 -> 4)
    categoricalFeaturesInfo += (1 -> 4)
    categoricalFeaturesInfo += (4 -> midMap.size)
    categoricalFeaturesInfo += (6 -> genderMap.size)
    categoricalFeaturesInfo += (7 -> pcodeMap.size)

    val numClasses = 2
    // Defning values for the other parameters
    val impurity = "gini"
    val maxDepth = 9
    val maxBins = 7000

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    //    println("Decision tree model:\n%s".format(model.toDebugString))

    //saving model
    val fos = new FileOutputStream("/root/IBMzSystems/model.obj")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(model)
    oos.close()

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction, point.features)
    }
    val showmedata = labelAndPreds.toDF()

  
    var features: Array[Double] = null
    var label: Double = 0.0
    var prediction: Double = 0.0
    showmedata.collect().foreach(row => {
      label = row.getAs[Double](0)
      prediction = row.getAs[Double](1)
      
      val tmp: DenseVector = row.getAs[DenseVector](2)
      features = tmp.toArray
      
      val fw1 = new FileWriter("/root/IBMzSystems/Historicalanalytics.txt", true)
      try {
        //val tmpstr = transaction.split(',')
        fw1.write(label + "," + prediction + "," + features(0) + "," + features(1) + "," + features(2) + "," + features(3) + "," + features(4) + "," + features(5) + "," + features(6) + "," + features(7) + "," + features(8) + "," + features(9))
        fw1.write("\n")
      }
      finally fw1.close()
    })
    
	val HistoryRDD = sc.textFile("file:///root/IBMzSystems/Historicalanalytics.txt").map(_.split(",")).map(p => History(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble, p(9).toDouble, p(10).toDouble, p(11).toDouble))

    val historyDF = HistoryRDD.toDF().cache()

    val uspostalRDD = sc.textFile("/u/dfz01/USA-Zip_csv.csv").map(_.split(",")).map(x => uspostal(x(0),x(1),x(2)))

    val uspostalDF = uspostalRDD.toDF()

    historyDF.registerTempTable("HistoryData")
    FraudsDF.registerTempTable("Frauds")
    uspostalDF.registerTempTable("uspostal")

    val HistoryAnalytics = sqlContext.sql("select HistoryData.label,HistoryData.prediction,Frauds.Time,Frauds.m_id as m_cat,Frauds.m_cat as m_id,Frauds.amount,Frauds.cid,Frauds.pcode from Frauds inner join HistoryData on Frauds.cid = HistoryData.customerid ").cache()

    HistoryAnalytics.registerTempTable("ReportingTable")

    val mapdata = sqlContext.sql("select ReportingTable.label,ReportingTable.prediction,ReportingTable.Time,ReportingTable.m_id as m_cat,ReportingTable.m_cat as m_id,ReportingTable.amount,ReportingTable.cid,ReportingTable.pcode,uspostal.State from ReportingTable inner join uspostal on ReportingTable.pcode = uspostal.pcode")
    mapdata.registerTempTable("mapfinaldata")


    mapdata.show()

    ////////////////////************************WRITE FINAL DATA INTO FILE*************************////////////
    var label1: Double = 0.0
    var prediction1: Double = 0.0
    var Time: String = ""
    var m_id1: Int = 0
    var m_cat1: String = ""
    var amount1: Double = 0.0
    var  cid1: Int = 0
    var pcode11: String = ""
    var State1: String = ""
    mapdata.collect().foreach(row => {
      label1 = row.getAs[Double](0)
      prediction1 = row.getAs[Double](1)
      Time = row.getAs[String](2)
      m_id1 = row.getAs[Int](3)
      m_cat1 = row.getAs[String](4)
      amount1 = row.getAs[Double](5)
      cid1 = row.getAs[Int](6)
      pcode11 = row.getAs[String](7)
      State1 = row.getAs[String](8)

      val fw1 = new FileWriter("/u/dfz01/FinalData/HistoricalData.txt", true)
      try {
        fw1.write(m_cat1 + "," + Time + "," + pcode11 + "," + label1 + "," + prediction1 + "," + amount1 + "," + cid1 + "," + m_id1 + "," + State1 )
        fw1.write("\n")
      }
      finally fw1.close()
    })
  }
}


