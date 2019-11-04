package org.ekstep.analytics.job


import org.apache.spark.sql.Encoders.kryo
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
case class UserSummary(accounts_validated: Long, accounts_rejected: Long,
                       accounts_unclaimed: Long, accounts_failed: Long) {
}

// Geo user summary in the json will have this POJO
case class GeoSummary(districts: Long, blocks: Long, school: Long) {

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
    val locationDF = loadData(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace)).select(
      col("id").as("locid"),
      col("code").as("code"),
      col("name").as("name"),
      col("parentid").as("parentid"),
      col("type").as("type")
    )
    val distinctChannelDF = loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace)).select(col = "channel").distinct()
    val activeRootOrganisationDF = loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
        .select(col("id"), col("channel"))
        .where(col("isrootorg") && col("status").=== (1))

    JobLogger.log(s"Active root org count = $activeRootOrganisationDF.count()")

    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"

    val fSFileUtils = new HDFSFileUtils(className, JobLogger)

    val detailDir = s"${tempDir}/detail"
    val summaryDir = s"${tempDir}/summary"

    case class ShadowUserData(channel: String)
    case class RootOrgData(id: Long, channel: String)

    implicit def ShadowDataEncoder = kryo[ShadowUserData]
    implicit def RootOrgDataEncoder = kryo[RootOrgData]

    // For distinct channel names, do the following:
    // 1. Create a json, csv report - user-summary.json, user-detail.csv.

    // FIXME: Remove these commented lines.
    //    distinctChannelDF.collect().foreach(rowUnit => {
    //      val channelName = rowUnit.mkString
    distinctChannelDF.map(channelData => {
      val channelName = channelData.get(0).toString
      JobLogger.log(s"Start working on $channelName")

      val oneOrgUsersDF = loadData(spark, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace))
        .where(col("channel").===(channelName))

      val countsDF = oneOrgUsersDF.select(
          col("claimstatus"),
          col(count("claimstatus")))
        .groupBy("claimstatus")

      countsDF.foreach(r => {
        val status = r.get(0)
        var itr = status.toString.toInt
        println(s"$status is the status and has ${r.getAs[Long](1)}")
        val counter = r.getAs[Long](1)

        val presentVal: UserStatus.Value = UserStatus(itr)

        presentVal match {
          case UserStatus.unclaimed => unclaimedCount = counter
          case UserStatus.claimed => claimedCount = counter
          case UserStatus.rejected => rejectedCount = counter
          // All these are considered failed.
          case UserStatus.failed => failedCount += counter
          case UserStatus.orgextidmismatch => failedCount += counter
          case UserStatus.multimatch => failedCount += counter
          case _ => println(s"Unknown status value = ${presentVal}")
        }
      })

      var summaryOutput = UserSummary(claimedCount, rejectedCount, unclaimedCount, failedCount)

      JobLogger.log(s"${channelName} has ${oneOrgUsersDF.cache().count()} many users in shadow_user table")

      val jsonStr = JSONUtils.serialize(summaryOutput)
      val rdd = spark.sparkContext.parallelize(Seq(jsonStr))
      val summaryDF = spark.read.json(rdd)

      saveUserDetailsReport(oneOrgUsersDF, s"${detailDir}/channel=${channelName}")
      saveSummaryReport(summaryDF, s"${summaryDir}/channel=${channelName}")
    })

    fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "user-detail")
    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "user-summary")

    // Purge the directories after copying to the upload staging area
    fSFileUtils.purgeDirectory(detailDir)
    fSFileUtils.purgeDirectory(summaryDir)

    // iterating through all active rootOrg and fetching all active suborg for a particular rootOrg
//    FIXME: Remove these commented lines.
    // activeRootOrganisationDF.collect().foreach(rowUnit => {
//      val rootOrgId = rowUnit.get(0).toString
//      val channelName = rowUnit.get(1).toString

    activeRootOrganisationDF.map(rootOrgData => {
      val rootOrgId = rootOrgData.get(0).toString
      val channelName = rootOrgData.get(1).toString
      JobLogger.log(s"RootOrg id found = ${rootOrgId} and channel = ${channelName}")

      // fetching all suborg for a particular rootOrg
      var schoolCountDf = loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
        .where(col("status").equalTo(1) && not(col("isrootorg"))
          && col("rootorgid").equalTo(rootOrgId))

      // getting count of suborg , that will provide me school count.
      val schoolCount = schoolCountDf.count()

      val locationExplodedSchoolDF = schoolCountDf
        .withColumn("exploded_location", explode(array("locationids")))

      // collecting District Details and count from organisation and location table
      val districtDF = locationExplodedSchoolDF
        .join(locationDF,
          col("exploded_location").cast("string")
            .contains(col("locid"))
           && locationDF.col("type") === "district")
      // collecting district count
      val districtCount = districtDF.count()

      // collecting block count and details.
      val blockDetailsDF = locationExplodedSchoolDF
        .join(locationDF,
          col("exploded_location").cast("string").contains(col("locid"))
            && locationDF.col("type") === "block")

      //collecting block count
      val blockCount = blockDetailsDF.count()

      val geoDetailsDF = districtDF.union(blockDetailsDF)
      //geoDetailsDF.show(false)

      var geoSummary = GeoSummary(districtCount, blockCount, schoolCount)
      val jsonStr = JSONUtils.serialize(geoSummary)
      val rdd = spark.sparkContext.parallelize(Seq(jsonStr))
      val summaryDF = spark.read.json(rdd)

      saveGeoDetailsReport(geoDetailsDF, s"${detailDir}/channel=${channelName}")
      saveSummaryReport(summaryDF, s"${summaryDir}/channel=${channelName}")
    })

    fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "geo-detail")
    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "geo-summary")

    // Purge the directories after copying to the upload staging area
    fSFileUtils.purgeDirectory(detailDir)
    fSFileUtils.purgeDirectory(summaryDir)

    JobLogger.log("Finished with prepareReport")
    distinctChannelDF
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
  def saveUserDetailsReport(reportDF: DataFrame, url: String): Unit = {
    // List of fields available
    //channel,userextid,addedby,claimedon,claimedstatus,createdon,email,name,orgextid,phone,processid,updatedon,userid,userids,userstatus

    reportDF.coalesce(1)
      .select(
        col("userextid").as("User external id"),
        col("userstatus").as("User account status"),
        col("userid").as("User id"),
        col("userids").as("Matching User ids"),
        col("claimedon").as("Claimed on"),
        col("orgextid").as("School external id"),
        col("claimedstatus").as("Claimed status"),
        col("createdon").as("Created on"),
        col("updatedon").as("Last updated on")
      )
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(url)

    JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
  }

  def saveGeoDetailsReport(reportDF: DataFrame, url: String): Unit = {
    reportDF.coalesce(1)
      .select(
        col("id").as("School id"),
        col("orgname").as("School name"),
        col("channel").as("Channel"),
        col("status").as("Status"),
        col("locid").as("Location id"),
        col("name").as("Location name"),
        col("parentid").as("Parent location id"),
        col("type").as("type")
      )
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
    val container = AppConf.getConfig("admin.reports.cloud.container")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")

    val storageService = StorageServiceFactory
      .getStorageService(StorageConfig(provider, AppConf.getStorageKey(provider), AppConf.getStorageSecret(provider)))
    storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
  }
}

