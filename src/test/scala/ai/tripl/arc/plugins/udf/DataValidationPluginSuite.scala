package ai.tripl.arc.plugins

import ai.tripl.arc.ARC
import ai.tripl.arc.api.API._
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.TestUtils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import ai.tripl.arc.udf.UDF


class DataValidationPluginSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    UDF.registerUDFs()(spark, logger, arcContext)

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

  test("DataValidationPluginSuite") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "plugins": {
        "config": []
      },
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${spark.getClass.getResource("/").toString}/basic.sql",
          "outputView": "customer",
          "persist": false,
        }
      ]
    }"""
    
    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    // assert graph created
    pipelineEither match {
      case Left(err) => {
        println(err)
        assert(false)
      }
      case Right((pipeline, _)) => {
        val row = ARC.run(pipeline)(spark, logger, arcContext).get.first
        
        assert(row.getBoolean(0) == true)
        assert(row.getString(1) == "+61499000000")
        assert(row.getBoolean(2) == true)
      }
    }
  }
  
  test("isValidPhoneNumber") {
    case class PhoneNumberValid(
      number: String,
      region: String,
      valid: Boolean,
    )

    val tests = List(
      PhoneNumberValid("61499000000", "AU", true),
      PhoneNumberValid("0499000000", "AU", true),
      PhoneNumberValid("61499000000", "US", false),
      PhoneNumberValid("888-452-1505", "US", true)
    )

    tests.foreach{ 
      t: PhoneNumberValid => {
        assert(ai.tripl.arc.plugins.udf.DataValidationPlugin.isValidPhoneNumber(t.number, t.region) == t.valid, s"${t.number} ${t.region}")
      }
    }
  }

  test("formatPhoneNumber") {
    case class PhoneNumberValid(
      number: String,
      region: String,
      valid: String,
    )

    val tests = List(
      PhoneNumberValid("61499000000", "AU", "+61499000000"),
      PhoneNumberValid("(555) 555-1234", "US", "+15555551234")
    )

    tests.foreach{ 
      t: PhoneNumberValid => {
        assert(ai.tripl.arc.plugins.udf.DataValidationPlugin.formatPhoneNumber(t.number, t.region) == t.valid, s"${t.number} ${t.region}")
      }
    }
  }  

  test("isValidABN") {
    case class ABNValid(
      abn: String,
      valid: Boolean,
    )

    val tests = List(
      ABNValid("83 914 571 673", true),
      ABNValid("83 914 571 672", false)
    )

    tests.foreach{ 
      t: ABNValid => {
        assert(ai.tripl.arc.plugins.udf.DataValidationPlugin.isValidABN(t.abn) == t.valid, s"${t.abn}")
      }
    }
  }  
}
