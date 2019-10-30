package org.ekstep.analytics.job

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.util.HDFSFileUtils
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

import scala.collection.{Map, _}

// Following values are possible in shadow_user.claimedstatus column.
object UserStatus extends Enumeration {
  type UserStatus = Value

  //  UNCLAIMED: 0 // initial Status <br>
  val unclaimed = Value(0, "UNCLAIMED")
  //  claimed: 1 // When the user is claimed.<br>
  val claimed = Value(1, "CLAIMED")
  //  REJECTED: 2 // when user says NO
  val rejected = Value(2, "REJECTED")
  //  FAILED: 3  // when user failed to verify the ext user id.
  val failed = Value(3, "FAILED")
  //  MULTIMATCH: 4 // when multiple user found with the identifier.<br>
  val multimatch = Value(4, "MULTIMATCH")
  //  ORGEXTIDMISMATCH: 5 // when provided ext org id is incorrect.<br>
  val orgextidmismatch = Value(5, "ORGEXTIDMISMATCH")
}

// Shadow user summary in the json will have this POJO
object UserSummary {
  var accounts_validated = 0L
  var accounts_rejected = 0L
  var accounts_unclaimed = 0L
  var accounts_failed = 0L
}


object StateAdminReportJob extends optional.Application with IJob with ReportGenerator {

  implicit val className = "org.ekstep.analytics.job.StateAdminReportJob"

  def name(): String = "StateAdminReportJob"

  private val DETAIL_STR = "detail"
  private val SUMMARY_STR = "summary"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {

    JobLogger.init(name())
    JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = 10

    def runJob(sc: SparkContext): Unit = {
      try {
        execute(jobConfig)(sc)
      } finally {
        CommonUtil.closeSparkContext()(sc)
      }
    }

    sc match {
      case Some(value) => {
        implicit val sparkContext: SparkContext = value
        runJob(value)
      }
      case None => {
        val sparkCassandraConnectionHost =
          jobConfig.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkCassandraConnectionHost")
        val sparkElasticsearchConnectionHost =
          jobConfig.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkElasticsearchConnectionHost")
        implicit val sparkContext: SparkContext =
          CommonUtil.getSparkContext(JobContext.parallelization,
            jobConfig.appName.getOrElse(jobConfig.model), sparkCassandraConnectionHost, sparkElasticsearchConnectionHost)
        runJob(sparkContext)
      }
    }
  }

  private def execute(config: JobConfig)(implicit sc: SparkContext) = {
    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val sparkConf = sc.getConf
      .set("es.write.operation", "upsert")
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val reportDF = prepareReport(spark, loadData)
    uploadReport(renamedDir)
    JobLogger.end("StateAdminReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
  }

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(settings)
      .load()
  }

  def prepareReport(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
    val distinctChannelDF = loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace)).select(col = "channel").distinct()

    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"

    val fSFileUtils = new HDFSFileUtils(className, JobLogger)

    val detailDir = s"${tempDir}/detail"
    val summaryDir = s"${tempDir}/summary"

    // For distinct channel names, do the following:
    // 1. Create a json, csv report - user-summary.json, user-detail.csv.
    distinctChannelDF.collect().foreach(rowUnit => {
      var summaryOutput = UserSummary

      var channelName = rowUnit.mkString
      JobLogger.log(channelName)

      val oneOrgUsersDF = loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace))
          .where(col("channel").===(channelName))
      val unclaimedCount = oneOrgUsersDF.where(s"claimedstatus == ${UserStatus.unclaimed.id}").count()
      val claimedCount = oneOrgUsersDF.where(s"claimedstatus == ${UserStatus.claimed.id}").count()
      val rejectedCount = oneOrgUsersDF.where(s"claimedstatus == ${UserStatus.rejected.id}").count()
      val failedCount = oneOrgUsersDF.where(s"claimedStatus >= ${UserStatus.failed.id}").count()

      summaryOutput.accounts_validated = claimedCount
      summaryOutput.accounts_failed = failedCount
      summaryOutput.accounts_rejected = rejectedCount
      summaryOutput.accounts_unclaimed = unclaimedCount

      JobLogger.log(s"${rowUnit.mkString} has ${oneOrgUsersDF.cache().count()} many users in shadow_user table")

      val jsonStr = JSONUtils.serialize(summaryOutput)
      val rdd = spark.sparkContext.parallelize(Seq(jsonStr))
      val summaryDF = spark.read.json(rdd)

      saveDetailsReport(oneOrgUsersDF, s"${detailDir}/channel=${channelName}")
      saveSummaryReport(summaryDF, s"${summaryDir}/channel=${channelName}")
    })

    fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "user-detail")
    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "user-summary")
    //uploadReport(renamedDir)


    // TODO Geo-data

    return distinctChannelDF
  }


  def saveReportES(reportDF: DataFrame): Unit = {
  }

  def saveReport(reportDF: DataFrame, url: String): Unit = {

  }

  /**
    * Saves the raw data as a .csv.
    * Appends /detail to the URL to prevent overwrites.
    * Check function definition for the exact column ordering.
    * @param reportDF
    * @param url
    */
  def saveDetailsReport(reportDF: DataFrame, url: String): Unit = {
    reportDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(url)

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
  }

  /**
    * Saves the raw data as a .json.
    * Appends /summary to the URL to prevent overwrites.
    * Check function definition for the exact column ordering.
    * * If we don't partition, the reports get subsequently updated and we dont want so
    * @param reportDF
    * @param url
    */
  def saveSummaryReport(reportDF: DataFrame, url: String): Unit = {
    reportDF.coalesce(1)
      .write
      .mode("overwrite")
      .json(url)

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
    println(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()} and ${url}")
  }

  def uploadReport(sourcePath: String) = {
    val provider = AppConf.getConfig("course.metrics.cloud.provider")

    // Container name can be generic - we dont want to create as many container as many reports
    val container = AppConf.getConfig("course.metrics.cloud.container")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")

    val storageService = StorageServiceFactory
      .getStorageService(StorageConfig(provider, AppConf.getStorageKey(provider), AppConf.getStorageSecret(provider)))
    storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
  }
}

