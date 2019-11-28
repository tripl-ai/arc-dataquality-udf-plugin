package ai.tripl.arc.plugins

import ai.tripl.arc.ARC
import ai.tripl.arc.api.API._
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.config.Error._
import ai.tripl.arc.util.TestUtils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import ai.tripl.arc.udf.UDF


class DataQualitySuite extends FunSuite with BeforeAndAfter {

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

  test("DataQualitySuite") {
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
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val row = ARC.run(pipeline)(spark, logger, arcContext).get.first
        
        assert(row.getBoolean(0) == true)
        assert(row.getBoolean(1) == false)
        assert(row.getString(2) == "+61499000000")
        assert(row.getBoolean(3) == true)
        assert(row.getBoolean(4) == false)
        assert(row.getBoolean(5) == true)
        assert(row.getBoolean(6) == false)        
      }
    }
  }
  
  test("isValidPhoneNumber") {
    case class PhoneNumberValid(
      number: String,
      region: String,
      valid: Boolean
    )

    val tests = List(
      PhoneNumberValid("61499000000", "AU", true),
      PhoneNumberValid("0499000000", "AU", true),
      PhoneNumberValid("61499000000", "US", false),
      PhoneNumberValid("888-452-1505", "US", true)
    )

    tests.foreach{ 
      t: PhoneNumberValid => {
        assert(ai.tripl.arc.plugins.udf.DataQualityPlugin.isValidPhoneNumber(t.number, t.region) == t.valid, s"${t.number} ${t.region}")
      }
    }
  }

  test("formatPhoneNumber") {
    case class PhoneNumberValid(
      number: String,
      region: String,
      valid: Option[String]
    )

    val tests = List(
      PhoneNumberValid("61499000000", "AU", Option("+61499000000")),
      PhoneNumberValid("(555) 555-1234", "US", None),
      PhoneNumberValid("61499000000", "US", None)
    )

    tests.foreach{ 
      t: PhoneNumberValid => {
        t.valid match {
          case Some(valid) => assert(ai.tripl.arc.plugins.udf.DataQualityPlugin.formatPhoneNumber(t.number, t.region) == valid, s"${t.number} ${t.region}")
          case None => {
            val thrown0 = intercept[Exception] {
              ai.tripl.arc.plugins.udf.DataQualityPlugin.formatPhoneNumber(t.number, t.region)
            }
            assert(thrown0.getMessage === "Cannot format invalid phone number.")
          }
        }
        
      }
    }
  }  

  test("isValidABN") {
    case class ABNValid(
      abn: String,
      valid: Boolean
    )

    val tests = List(
      ABNValid("83 914 571 673", true),
      ABNValid("83 914 571 672", false),
      ABNValid("83 914 571 67", false),
      ABNValid("83 914 571 67a", false)
    )

    tests.foreach{ 
      t: ABNValid => {
        assert(ai.tripl.arc.plugins.udf.DataQualityPlugin.isValidABN(t.abn) == t.valid, s"${t.abn}")
      }
    }
  }  

  test("isValidACN") {
    case class ACNValid(
      acn: String,
      valid: Boolean
    )

    val tests = List(
      ACNValid("000 000 019", true),
      ACNValid("000 250 000", true),
      ACNValid("000 500 005", true),
      ACNValid("000 750 005", true),
      ACNValid("001 000 004", true),
      ACNValid("001 250 004", true),
      ACNValid("001 500 009", true),
      ACNValid("001 749 999", true),
      ACNValid("001 999 999", true),
      ACNValid("002 249 998", true),
      ACNValid("002 499 998", true),
      ACNValid("002 749 993", true),
      ACNValid("002 999 993", true),
      ACNValid("003 249 992", true),
      ACNValid("003 499 992", true),
      ACNValid("003 749 988", true),
      ACNValid("003 999 988", true),
      ACNValid("004 249 987", true),
      ACNValid("004 499 987", true),
      ACNValid("004 749 982", true),
      ACNValid("004 999 982", true),
      ACNValid("005 249 981", true),
      ACNValid("005 499 981", true),
      ACNValid("005 749 986", true),
      ACNValid("005 999 977", true),
      ACNValid("006 249 976", true),
      ACNValid("006 499 976", true),
      ACNValid("006 749 980", true),
      ACNValid("006 999 980", true),
      ACNValid("007 249 989", true),
      ACNValid("007 499 989", true),
      ACNValid("007 749 975", true),
      ACNValid("007 999 975", true),
      ACNValid("008 249 974", true),
      ACNValid("008 499 974", true),
      ACNValid("008 749 979", true),
      ACNValid("008 999 979", true),
      ACNValid("009 249 969", true),
      ACNValid("009 499 969", true),
      ACNValid("009 749 964", true),
      ACNValid("009 999 964", true),
      ACNValid("010 249 966", true),
      ACNValid("010 499 966", true),
      ACNValid("010 749 961", true),
      ACNValid("000 000 010", false),
      ACNValid("000 250 001", false),
      ACNValid("000 500 006", false),
      ACNValid("000 750 006", false),
      ACNValid("001 000 005", false),
      ACNValid("001 250 005", false),
      ACNValid("001 500 000", false),
      ACNValid("001 749 990", false),
      ACNValid("001 999 990", false),
      ACNValid("002 249 999", false),
      ACNValid("002 499 999", false),
      ACNValid("002 749 994", false),
      ACNValid("002 999 994", false),
      ACNValid("003 249 993", false),
      ACNValid("003 499 993", false),
      ACNValid("003 749 989", false),
      ACNValid("003 999 989", false),
      ACNValid("004 249 988", false),
      ACNValid("004 499 988", false),
      ACNValid("004 749 983", false),
      ACNValid("004 999 983", false),
      ACNValid("005 249 982", false),
      ACNValid("005 499 982", false),
      ACNValid("005 749 987", false),
      ACNValid("005 999 978", false),
      ACNValid("006 249 977", false),
      ACNValid("006 499 977", false),
      ACNValid("006 749 981", false),
      ACNValid("006 999 981", false),
      ACNValid("007 249 980", false),
      ACNValid("007 499 980", false),
      ACNValid("007 749 976", false),
      ACNValid("007 999 976", false),
      ACNValid("008 249 975", false),
      ACNValid("008 499 975", false),
      ACNValid("008 749 970", false),
      ACNValid("008 999 970", false),
      ACNValid("009 249 960", false),
      ACNValid("009 499 960", false),
      ACNValid("009 749 965", false),
      ACNValid("009 999 965", false),
      ACNValid("010 249 967", false),
      ACNValid("010 499 967", false),
      ACNValid("010 749 962", false),     
      ACNValid("010 749 96", false),     
      ACNValid("010 749 96a", false)     
    )

    tests.foreach{ 
      t: ACNValid => {
        assert(ai.tripl.arc.plugins.udf.DataQualityPlugin.isValidACN(t.acn) == t.valid, s"${t.acn}")
      }
    }
  }    
}
