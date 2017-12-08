package com.kal.datafactz
/**
  * Trasactions Consumer , Streaming and Retail Topic Producer APP
  */
import java.io.{FileInputStream, FileWriter, ObjectInputStream}

import consumer.kafka.ReceiverLauncher
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json
import org.json.JSONObject


object KafkaConsumer {

	//Schema Design for Trasaction Data
  case class Fraud(imp: String, potential: String, m_id: String, auth: String, age: Int, Time: String, gender: String, name: String, pcode: String, cid: Double, m_cat: Double, amount: Double, marital: Double, edu: Double, children: Double, cardpan: Double, res: Int)

  def parseFraud(str: String): Fraud = {
    val line = str.split(",")
    Fraud(line(0), line(1), line(2), line(3), line(4).toInt, line(5), line(6), line(7), line(8), line(9).toDouble, line(10).toDouble, line(11).toDouble, line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble, line(16).toInt)
  }

  case class TransFraud(fraudLabel: Double, prediction: Double, Time: String, merchant_cat: String, mid: Int, amountf: Double, customerid: Int, pcode1: String, State: String)

  def parseTransFraud(str: String): TransFraud = {
    val line = str.split(",")
    TransFraud(line(3).toDouble, line(4).toDouble, line(1), line(0) /*merchant_cat*/ , line(7).toInt /*(mid)*/ , line(5).toDouble /*amountf*/ ,
      line(6).toInt, line(2) /* pcode1*/ , line(8))
  }

  case class uspostal(pcode: String, State: String, Statecode: String)

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.{Level, Logger}
	//Logging-OFF
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Create SparkContext
    val conf = new SparkConf()
      .setAppName("Fraud Streaming")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.streaming.unpersist", "true")
	//Spark Context and Spark Streaming Context
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val sqlContext = new SQLContext(sc)
	
	//ZooKeeper Properties
    val topic = args(0)
    val zkhosts = "54.172.88.199"
    val zkports = "2181"
    val brokerPath = "/brokers"

    //Specify number of Receivers you need.
    val numberOfReceivers = 1

    //The number of partitions for the topic will be figured out automatically
    //However, it can be manually specified by adding kafka.partitions.number property
    val kafkaProperties: Map[String, String] = Map("zookeeper.hosts" -> zkhosts,
      "zookeeper.port" -> zkports,
      "zookeeper.broker.path" -> brokerPath,
      "kafka.topic" -> topic,
      "zookeeper.consumer.connection" -> "54.172.88.199:2181",
      "zookeeper.consumer.path" -> "/spark-kafka", "kafka.consumer.id" -> "12345")


    val props = new java.util.Properties()
    kafkaProperties foreach { case (key, value) => props.put(key, value) }


    //Producer Properties
    val producderProps = new java.util.Properties()
    producderProps.put("metadata.broker.list", "54.172.88.199:9092")
    producderProps.put("producer.type", "sync")
    producderProps.put("compression.codec", "2")
    producderProps.put("serializer.class", "kafka.serializer.StringEncoder")
    producderProps.put("client.id", "camus");

    val producerConfig = new ProducerConfig(producderProps)
    val producer = new Producer[String, String](producerConfig)

    //Producerend

    val tmp_stream = ReceiverLauncher.launch(ssc, props, numberOfReceivers, StorageLevel.MEMORY_AND_DISK_SER)

    //Load saved Model
    val fos = new FileInputStream("/u/dfz01/model.obj")
    val oos = new ObjectInputStream(fos)
    val model = oos.readObject().asInstanceOf[org.apache.spark.mllib.tree.model.DecisionTreeModel]

    import sqlContext.implicits._
	//Mapping State Names
    val uspostalRDD = sc.textFile("/u/dfz01/USA-Zip_csv.csv").map(_.split(",")).map(x => uspostal(x(0), x(1), x(2)))
    val uspostalDF = uspostalRDD.toDF().cache()
    uspostalDF.registerTempTable("postalcodestable")
	
    tmp_stream.foreachRDD(rdd => {

      rdd.collect().foreach(msg => {
        val payload = msg.getPayload
        val transaction = new String(payload)
        val timestamp = System.currentTimeMillis().toString
        val filepath = "/u/dfz01/trns/" + timestamp

        val fr = new FileWriter(filepath)
        try {
          fr.write(transaction)
          fr.write("\n")
        }
        fr.close()
        
	def parseFraud(str: String): Fraud = {
          val line = str.split(",")
          Fraud(line(0), line(1), line(2), line(3), line(4).toInt, line(5), line(6), line(7), line(8), line(9).toDouble, line(10).toDouble, line(11).toDouble, line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble, line(16).toInt)
        }

        val textRDD = sc.textFile(filepath)
        val FraudsRDD = textRDD.map(parseFraud)

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
		val labelAndPreds = mldata.map { point =>
          val prediction = model.predict(point.features)
          (point.label, prediction, point.features)
        }

        //my streming data with fraud flag
        val mysteamdata = labelAndPreds.toDF()

        var features: Array[Double] = null
        var label: Double = 0.0
        var prediction: Double = 0.0
        mysteamdata.collect().foreach(row => {
          label = row.getAs[Double](0)
          prediction = row.getAs[Double](1)
         
          val tmp: DenseVector = row.getAs[DenseVector](2)
          features = tmp.toArray
        })

        //Final process
        val tmpstr = transaction.split(",")

        val stateCodeR = sqlContext.sql("SELECT State FROM postalcodestable WHERE pcode='" + tmpstr(8) + "'")
        var stateCode: Any = null;
        stateCodeR.collect() foreach (r => {
          stateCode = r.get(0)
        })
        val fw1 = new FileWriter("/u/dfz01/FinalData/analytics.txt", true)
        try {
          fw1.write(tmpstr(2) + "," + tmpstr(5) + "," + tmpstr(8) + "," + label + "," + prediction + "," + tmpstr(11) + "," + tmpstr(9) /* customerid */ + "," + tmpstr(10)
            + "," + stateCode)
          fw1.write("\n")
        }
        finally fw1.close()

        //println("In the start process")
        val textRDD1 = sc.textFile("/u/dfz01/FinalData/")
        val FinalRDD = textRDD1.map(parseTransFraud).cache()

        val FraudsDF = FinalRDD.toDF()

        //Register as table
        FraudsDF.registerTempTable("reportingTable")

        // Final metrics calculation
        println("--------------------Print All Metrics------------------------------")

        val CTCount = sqlContext.sql("select count(*) totalTrns from reportingTable")
        CTCount.registerTempTable("TotalTran")
        val PFNCount = sqlContext.sql("select count(*) as PredictedFalseNegativesCount  from reportingTable where fraudLabel=0 and prediction=1")
        val PFPCount = sqlContext.sql("select count(*) as PredictedFalsePositiveCount  from reportingTable where fraudLabel=1 and prediction=0")
        val PFNamount = sqlContext.sql("select sum(amountf) as PredictedFalseNegativesAmount  from reportingTable where fraudLabel=0 and prediction=1")
        val PFPamount = sqlContext.sql("select sum(amountf) as PredictedFalsePositiveAmount  from reportingTable where fraudLabel=1 and prediction=0")
        val AFAmount = sqlContext.sql("select sum(amountf) as ActualFraudAmount  from reportingTable where prediction=1")
        val AFMdf = sqlContext.sql("select merchant_cat,Time,count(merchant_cat) as totalvalue from reportingTable where fraudLabel=1 group by merchant_cat,Time order by totalvalue desc limit 10")
        val FraudFPbyState = sqlContext.sql("select State, count(*) as TotalFlasePositives from reportingTable where fraudLabel=1 and prediction=0 group by State")
        val FraudNPbyState = sqlContext.sql("select State, count(*) as TotalFlaseNagative from reportingTable where fraudLabel=0 and prediction=1 group by State")

        val FraudCountbyState = sqlContext.sql("select State, count(*) as TotalFraudCount from reportingTable where prediction=1 group by State")

        val CFCount = sqlContext.sql("select sum(cumm_total) cumm_total, sum(cumm_fr_txns) cumm_fr_txns, (sum(cumm_total)/max(totalTrns))*100 cumm_percent, " +
          "(sum(cumm_fr_txns)/max(totalTrns))*100 cumm_fr_percent from (select count(*) cumm_total, sum(case when fraudLabel= 1 then 1 else 0 end) as cumm_fr_txns," +
          " customerid from reportingTable group by customerid having count(*) > 2) x join TotalTran")

        var cumm_total: Any = null
        var cumm_fr_txns: Any = null
        var cumm_percent: Any = null
        var cumm_fr_percent: Any = null
        CFCount.collect().foreach(r => {
          cumm_total = r.get(0)
          cumm_fr_txns = r.get(1)
          cumm_percent = r.get(2)
          cumm_fr_percent = r.get(3)
        })

        FraudFPbyState.registerTempTable("FraudFPbyState")
        FraudNPbyState.registerTempTable("FraudNPbyState")
        FraudCountbyState.registerTempTable("FraudCountbyState")

        val FraudDetails = sqlContext.sql("select FraudFPbyState.State,FraudNPbyState.TotalFlaseNagative, FraudFPbyState.TotalFlasePositives, FraudCountbyState.TotalFraudCount" +
          " from FraudFPbyState join FraudNPbyState on FraudFPbyState.State = FraudNPbyState.State join FraudCountbyState on FraudNPbyState.State = FraudCountbyState.State")
		
		//JSON Preparation
        val rootJSON = new JSONObject()
        val mainJSON = new JSONObject()
        mainJSON.put("cumulative_fraud_transactions", cumm_fr_txns)
        mainJSON.put("cumulative_fraud_transactions_percent", cumm_fr_percent)
        mainJSON.put("cumulative_transactions", cumm_total)
        mainJSON.put("cumulative_transactions_percent", cumm_percent)

        val FalsePredictionsJson = new JSONObject()
        var tmp: Any = null;
        PFNCount.collect().foreach(r => {
          tmp = r.get(0)
        })
        FalsePredictionsJson.put("no_of_false_negatives", tmp)
        PFPCount.collect().foreach(r => {
          tmp = r.get(0)
        })
        FalsePredictionsJson.put("no_of_false_positives", tmp)
        mainJSON.put("false_predictions", FalsePredictionsJson)

        val falsePredictionsAmount = new JSONObject()
        PFNamount.collect().foreach(r => {
          tmp = r.get(0)
        })
        falsePredictionsAmount.put("predicted_false_negative", tmp)
        PFPamount.collect().foreach(r => {
          tmp = r.get(0)
        })
        if (tmp == null)
          falsePredictionsAmount.put("predicted_false_positive", 0)
        else
          falsePredictionsAmount.put("predicted_false_positive", tmp)

        mainJSON.put("false_predictions_amount", falsePredictionsAmount)


        val stateJO = new JSONObject()
        var state: Any = null
        FraudDetails.collect().foreach(r => {
          val stateDetails = new JSONObject()
          state = r.get(0)
          stateDetails.put("falseNegatives", r.get(1))
          stateDetails.put("falsePositives", r.get(2))
          stateDetails.put("totalFrauds", r.get(3))
          stateJO.put(state.toString(), stateDetails)
        })

        mainJSON.put("fraud_transactions_map", stateJO)
        val top10JA = new json.JSONArray()

        AFMdf.collect().foreach(r => {
          val top10JO = new JSONObject()
          top10JO.put("count", r.get(2))
          top10JO.put("date_affected", r.get(1))
          top10JO.put("merchange_category", r.get(0))
          top10JA.put(top10JO)
        })

        mainJSON.put("top10_affected_merchants", top10JA)

        /* BAR GRAPH CODE */
       //For time period slot of 12
        val timeSlot = sc.parallelize(1 to 24).toDF("hh").cache()
        timeSlot.registerTempTable("timetable")

        val ActualFraudBAR = sqlContext.sql("select th.hh as Hour, count(*) actual_fraud from timetable th left outer join reportingTable rt on th.hh = hour(cast(Time as timestamp)) where (unix_timestamp()-unix_timestamp(Time))  <= 43200 and fraudLabel = 1 and prediction = 1 group by th.hh")
        val FalseNagativesBAR = sqlContext.sql("select th.hh as Hour, count(*) false_negative_count from timetable th left outer join reportingTable rt on th.hh = hour(cast(Time as timestamp)) where (unix_timestamp()-unix_timestamp(Time))  <= 43200  and fraudLabel = 0 and prediction = 1 group by th.hh")
        val FalsePositivesBR = sqlContext.sql("select th.hh as Hour, count(*) false_positive_count from timetable th left outer join reportingTable rt on th.hh = hour(cast(Time as timestamp)) where (unix_timestamp()-unix_timestamp(Time))  <= 43200  and fraudLabel = 1 and prediction = 0 group by th.hh")

        ActualFraudBAR.registerTempTable("ActualFraudBAR")
        FalseNagativesBAR.registerTempTable("FalseNagativesBAR")
        FalsePositivesBR.registerTempTable("FalsePositivesBR")

        val BarGraphDetails = sqlContext.sql("select concat(cast(fn.Hour as varchar(2)),':00') Hour, actual_fraud, false_negative_count, false_positive_count from FalseNagativesBAR fn left outer join FalsePositivesBR fp on fn.Hour = fp.Hour left outer join ActualFraudBAR a on a.Hour = fn.Hour order by fn.Hour")

        val graphJA = new json.JSONArray()
        BarGraphDetails.collect().foreach(r => {
          val graphData = new JSONObject()
          graphData.put("time", r.get(0))
          graphData.put("falseNegatives", r.get(2))
          graphData.put("falsePositives", r.get(3))
          graphData.put("totalFrauds", r.get(1))
          graphJA.put(graphData)
        })

        val graphJO = new JSONObject()
        graphJO.put("data", graphJA)
        mainJSON.put("fraud_transactions_barline", graphJO)

        /* BAR GRAPH CODE */
		rootJSON.put("transactions_analytics_data", mainJSON)

        println("Result JSON: " + rootJSON.toString)

        val data = new KeyedMessage[String, String]("retail", rootJSON.toString())
//        println("Produced message ===>" + data.toString())
        producer.send(data)

      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}