// Databricks notebook source
val onlineshopperDF = sqlContext.read.format("csv")

  .option("header", "true")

  .option("inferSchema", "true")

  .option("delimiter", ",")

  .load("/FileStore/tables/online_shoppers_intention.csv")



display(onlineshopperDF)

// import the dataset into the dataframe and display the dataframe
// read the csv file and load the csv file contents into the dataframe first
// import the dataset into the onlineShopper data frame

// COMMAND ----------

onlineshopperDF.printSchema();

//display the schema details of the columns details

// COMMAND ----------

onlineshopperDF.select("Administrative", "Administrative_Duration", "Informational", "Informational_Duration", "ProductRelated", "ProductRelated_Duration", "BounceRates", "ExitRates", "PageValues", "SpecialDay", "Month", "OperatingSystems", "Browser", "Region", "TrafficType", "VisitorType", "Weekend", "Revenue").describe().show()

// select these columns that contains the numerical data and display the mean, std deviation, min, max, count value
// descriptive analytics (what happened?)

// COMMAND ----------

onlineshopperDF.createOrReplaceTempView("WebData")

// create temporary view table object from the dataframe to run spark sql statements later

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select * from WebData;
// MAGIC
// MAGIC -- select all columns from WebData table and display all records

// COMMAND ----------

// MAGIC %sql
// MAGIC select Revenue as CustomerboughtProduct, count(Revenue) as counts from WebData group 
// MAGIC by Revenue;
// MAGIC
// MAGIC -- how many visitors bought from us and how many customer did not buy from us? Why?
// MAGIC -- mostly did not buy, reason ... usibility of website/price expensive
// MAGIC -- first business question

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select Revenue as Customerwillbuy, count(Revenue) as counts from WebData group by Revenue;
// MAGIC
// MAGIC -- same as above business question, but we can present the results in pie chart format later

// COMMAND ----------

// MAGIC %sql
// MAGIC select Administrative, Administrative_Duration, Informational, Informational_Duration, ProductRelated, ProductRelated_Duration, BounceRates, ExitRates, PageValues, SpecialDay, Month, OperatingSystems, Browser, Region, TrafficType, VisitorType, Weekend, Revenue from WebData;
// MAGIC
// MAGIC -- generate heatmap to see the correl for all these columns data

// COMMAND ----------

// MAGIC %sql
// MAGIC select Weekend, Count(Weekend) from WebData group by Weekend;
// MAGIC
// MAGIC --how many visitors visit during weekend? Why?
// MAGIC --how many visitors visit during non-weekend? Why?
// MAGIC -- weekends prefer physical shopping
// MAGIC --weekdays after work tired, prefer online shopping
// MAGIC -- Second business question

// COMMAND ----------

// MAGIC %sql
// MAGIC select VisitorType, count(VisitorType) from WebData group by VisitorType
// MAGIC
// MAGIC --what types of visitors do we have? Why?
// MAGIC --most of the visitors are returning visitors, but they did not buy from us? Why?
// MAGIC --answer: rude customer service, product good but price expensive, lack of stock
// MAGIC --what to do to convert potential customers to real customers?
// MAGIC -- Third business question

// COMMAND ----------

// MAGIC %sql
// MAGIC select Browser, Count(Browser) as BrowserType from WebData group by Browser;
// MAGIC
// MAGIC --what type of web browser the visitor used, most popular to least popular? Why?
// MAGIC --2: Google chrome, 1: firefox
// MAGIC -- Forth business question

// COMMAND ----------

// MAGIC %sql
// MAGIC select TrafficType, count(TrafficType) from WebData group by TrafficType order by TrafficType;
// MAGIC
// MAGIC -- which traffic type is the most popular to the least popular? Why?
// MAGIC -- You can make your own assumption here for traffic type (direct, email, sms, referral, social, organic, ...)
// MAGIC --- Fifth business question

// COMMAND ----------

// MAGIC %sql
// MAGIC select Region, count(Region) from WebData group by Region;
// MAGIC
// MAGIC -- visitor comes from which region? Most popular to least popular region? Why?
// MAGIC -- you can make your own assumption on the region, region 1=? for example like above traffic type
// MAGIC -- region 1 is the most popular one and region 5 is the least popular one, why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select OperatingSystems, count(OperatingSystems) from WebData group by OperatingSystems;
// MAGIC
// MAGIC -- which is the most popular OS used by the visitors or least popular, why?
// MAGIC -- you can make your own assumptions here, for example, OS 1 = ? 
// MAGIC -- the most popular OS is 2, followed by 1, why? the top 3 OS is 2,1,3 and why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select Month, count(Month) from WebData group by Month;
// MAGIC
// MAGIC -- which month is the most busy month or least busy month? Why? The peak season falls in which month?
// MAGIC -- the off peak season fall in which month?

// COMMAND ----------

// MAGIC %sql
// MAGIC select Informational_Duration, Revenue from Webdata group by Informational_Duration,Revenue;
// MAGIC
// MAGIC -- how long the visitors spend on information related websites and does it affect the revenues?
// MAGIC -- what is the correlation and why? No, the amount of time they spend on the informational related websites does not really affect their purchase decisions, most of them who spend a lot of time on the informational websites does not make a purchase compared with those who spend kesser time but they make a purchase
// MAGIC -- people who spend lesser time on the informational related websites did make a purchase here
// MAGIC -- you need to find out why? (diagnostic analysis)

// COMMAND ----------

// MAGIC %sql
// MAGIC select Administrative_Duration,Revenue from Webdata group by Administrative_Duration,Revenue;
// MAGIC
// MAGIC --does the amount of time the visitor spends on the administrative related website affect their purchase decisions? Why?
// MAGIC -- No, why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select ProductRelated_Duration,Revenue from Webdata group by ProductRelated_Duration,Revenue;
// MAGIC
// MAGIC -- does the amount of time the visitors spend on the product related website affect their purchase decisions? Why?
// MAGIC -- same as above, no, why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select ExitRates,Revenue from Webdata group by ExitRates,Revenue;
// MAGIC
// MAGIC -- does the exit rate affect the visitor purchase decisions or not?
// MAGIC -- high percentage of exit rate means how many times they view back the same webpage
// MAGIC -- it seems that the visitor open the same wepage again but still did not make any purchases? Why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select PageValues,Revenue from Webdata group by PageValues,Revenue;
// MAGIC
// MAGIC -- does the page values affect the visitor purchase decisions? Why?
// MAGIC -- higher page values lead to the higher prospect of purchases, why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select BounceRates,Revenue from Webdata group by BounceRates,Revenue;
// MAGIC
// MAGIC -- does the bounce rate affects the visitor purchase decisions? Why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select TrafficType, count(TrafficType), Revenue from Webdata group by TrafficType,Revenue;
// MAGIC
// MAGIC -- does the traffic type affect the visitor purchase decisions, are they correlated?
// MAGIC -- you can look at the most popular traffic type, type 1, 2, 3 first

// COMMAND ----------

// MAGIC %sql
// MAGIC select VisitorType, count(VisitorType), Revenue from Webdata group by VisitorType,Revenue;
// MAGIC
// MAGIC -- does the types of visitor affect the purchase decisions, any correlation? Why?
// MAGIC -- returning visitor: mostly do not make purchases

// COMMAND ----------

// MAGIC %sql
// MAGIC select Region, count(Region), Revenue from Webdata group by Region,Revenue;
// MAGIC
// MAGIC -- does the region type affect the visitor purchase decisions? Why?
// MAGIC -- you may look at the correlation for the most popular region 

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select Administrative, Informational from WebData;
// MAGIC
// MAGIC -- what is the correlation between administrative and informational web page?

// COMMAND ----------

// predictive analytics
// logistic regression model (S-curve, result is 0 /1)

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._



import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler

// COMMAND ----------

var StringfeatureCol = Array("Month", "VisitorType")

// step 3 data preprocessing, before we apply any predictive models on the dataset
// we need to clean the data first
// our dataset consists of these two string columns data that required some preprocessing same like the project 1 example
// we take these two columns from the dataset and put inside the new array object first

// COMMAND ----------

import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}


val indexers = StringfeatureCol.map { colName =>
  new StringIndexer().setInputCol(colName).setOutputCol(colName + "_indexed")

}


val pipeline = new Pipeline()

                    .setStages(indexers)      


val ShopperDF = pipeline.fit(onlineshopperDF).transform(onlineshopperDF)

// data pipelining stages 
// part of the data preprocessing activities
// we perform remapping on the string data columns "month" and "visitorType"
// create two more new columns on the dataset with the indexed value
// which is the numeric data type
// use this new ShopperDF as the new dataset for the predictive models later
// same as the previous project 1, we use the same data preprocessing steps here

// COMMAND ----------

ShopperDF.printSchema()

// last 2 columns are new columns
// you will see the new columns created here, our model cannot process string data type
// Month_indexed: double (nullable = false)
// VisitorType_indexed: double (nullable = false)


// COMMAND ----------

val FinalShopperDF = ShopperDF

  .withColumn("RevenueInt",$"Revenue".cast("Int"))

  .withColumn("WeekendInt",$"Weekend".cast("Int"))

  // we need to convert these 2 columns from boolean data type to integer data type
  // because the model cannot process boolean data type
  // yes = 1 and no = 0 using integer
  // create two more new columns with the remap value 

// COMMAND ----------

FinalShopperDF.printSchema()
// we can see the new created columns as below
//RevenueInt: integer (nullable = true)
// WeekendInt: integer (nullable = true)
// we can enable the code below to display the dataset


// display(FinalShopperDF)
// show revenue and weekend integer value

// COMMAND ----------

val splits = FinalShopperDF.randomSplit(Array(0.7, 0.3))
val train = splits(0)
val test = splits(1)
val train_rows = train.count()
val test_rows = test.count()
println("Training Rows: " + train_rows + " Testing Rows: " + test_rows)

// we need to split the dataset into 70% training set and 30% testing set
// because we are using supervised learning model logistic regression that requires training data and testing data

// COMMAND ----------

val assembler = new VectorAssembler().setInputCols(Array("Administrative", "Administrative_Duration", "Informational", "Informational_Duration", "ProductRelated", "ProductRelated_Duration", "BounceRates", "ExitRates", "PageValues", "SpecialDay", "OperatingSystems", "Browser", "Region", "TrafficType", "VisitorType_indexed", "WeekendInt", "RevenueInt")).setOutputCol("features")



val training = assembler.transform(train).select($"features", $"RevenueInt".alias("label"))



training.show(false)

// select the required columns and set labels for the model
// we select all the required columns from the dataset and use vector assembler to combine all these columns into a single feature
// all these columns served as the predictor to predict the value of the label
// did customer make a purchase or not

// COMMAND ----------

val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(10).setRegParam(0.3)

val model = lr.fit(training)

println("Model Trained!")

// send the training data into the logistic regression model for learning purposes
// step 4 modelling stages
// learn how to predict the customers' response, did customer make a purchase or not
// to do the prediction in the future

// COMMAND ----------

val testing = assembler.transform(test).select($"features", $"RevenueInt".alias("trueLabel"))

testing.show(false)

// apply the logistic regression model on the testing data
// to do the prediction whether customer will make purchase or not
// apply what the machine has learned 
// step 4 modelling
// create a new column on the testing dataset called true label, actual value 
// for us to do the comparison later actual va predicted value 
// so that we can measure the accuracy of the prediction later 

// COMMAND ----------

val prediction = model.transform(testing)

val predicted = prediction.select("features", "prediction", "trueLabel")

predicted.show()

// apply model on the testing data and display the result and do comparison
// there are so many records here, we cannot compare one by one

// COMMAND ----------

val tp = predicted.filter("prediction == 1 AND truelabel == 1").count().toFloat

val fp = predicted.filter("prediction == 1 AND truelabel == 0").count().toFloat

val tn = predicted.filter("prediction == 0 AND truelabel == 0").count().toFloat

val fn = predicted.filter("prediction == 0 AND truelabel == 1").count().toFloat

  val metrics = spark.createDataFrame(Seq(

 ("TP", tp),

 ("FP", fp),

 ("TN", tn),

 ("FN", fn),

 ("Precision", tp / (tp + fp)),

 ("Recall", tp / (tp + fn)))).toDF("metric", "value")

metrics.show()

// step 5 we need to measure the accuracy of the model in doing the prediction
// we can use confusion matrix here for the classification model
// in project 1 we used RMSE for linear regression 
// in this project we use confusion matrix for logistic regression
// confusion matrix: true positive, false positive, false negative, true negative

// COMMAND ----------

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("trueLabel")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(prediction)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC we already completed all the 5 stages of CRISP_DM now the last stage deployment , prescirptive analytics this is the individual components of the assignments each member is required to write recommendations based on the findings since we can predict which customer will buy or not buy, what is your recommendation
// MAGIC

// COMMAND ----------


