package ai.tripl.arc.plugins.udf

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession

import ai.tripl.arc.util.log.logger.Logger
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.util.Utils

import com.google.i18n.phonenumbers.PhoneNumberUtil
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat

class DataQuality extends ai.tripl.arc.plugins.UDFPlugin {

  val version = ai.tripl.arc.plugins.udf.dataquality.BuildInfo.version

  // one udf plugin can register multiple user defined functions
  override def register()(implicit spark: SparkSession, logger: Logger, arcContext: ARCContext) = {

    // register custom UDFs via sqlContext.udf.register("funcName", func )
    spark.sqlContext.udf.register("is_valid_phonenumber", DataQualityPlugin.isValidPhoneNumber _ )
    spark.sqlContext.udf.register("format_phonenumber", DataQualityPlugin.formatPhoneNumber _ )
    spark.sqlContext.udf.register("is_valid_abn", DataQualityPlugin.isValidABN _ )
    spark.sqlContext.udf.register("is_valid_acn", DataQualityPlugin.isValidACN _ )

  }
}

object DataQualityPlugin {

  // uses google's libphonenumber to validate whether an input string phone number is valid for a specific region
  def isValidPhoneNumber(numberToParse: String, defaultRegion: String): Boolean = {
    try {
      val phoneUtil = PhoneNumberUtil.getInstance
      val parsed = phoneUtil.parse(numberToParse, defaultRegion)
      phoneUtil.isValidNumber(parsed)
    } catch {
      case e: Exception => false
    }
  }

  // uses google's libphonenumber to convert phone number to ISO E164 format https://en.wikipedia.org/wiki/E.164
  def formatPhoneNumber(numberToParse: String, defaultRegion: String): String = {
    val phoneUtil = PhoneNumberUtil.getInstance
    val parsed = phoneUtil.parse(numberToParse, defaultRegion)
    if (phoneUtil.isValidNumber(parsed)) {
      phoneUtil.format(parsed, PhoneNumberFormat.E164)
    } else {
      throw new Exception("Cannot format invalid phone number.")
    }
  }

  // validates whether an Australian Business Number (ABN) passes the inbuilt checksum function
  // Subtract 1 from the first (left) digit to give a new eleven digit number.
  // Multiply each of the digits in this new number by its weighting factor.
  // Sum the resulting 11 products.
  // Divide the total by 89, noting the remainder.
  // If the remainder is zero the number is valid.
  def isValidABN(abn: String): Boolean = {
    // replace all non-digit characters
    val cleanABN = abn.replaceAll("\\D", "")

    // must be 11 characters
    if (cleanABN.length == 11) {
      val weightFactor = List(10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19)

      // calculate and verify checksum
      cleanABN.zipWithIndex.foldLeft(0) {
        case (sum, (digit, index)) => {
          // Subtract 1 from the first (left) digit
          val value = if (index == 0) digit.toString.toInt - 1 else digit.toString.toInt
          sum + (weightFactor(index) * value)
        }
      } % 89 == 0
    } else {
      false
    }
  }

  // validates whether an Australian Company Number (ACN) passes the inbuilt checksum function
  // Split the input ACN to remove the last digit which is the checksum digit
  // Multiply each of the remaining digits in this new number by its weighting factor.
  // Sum the resulting 8 products.
  // Divide the total by 10, noting the remainder.
  // if 10 - remainder = 10 then set to 0
  // if remainder = checksum digit then the number is valid.
  def isValidACN(acn: String): Boolean = {
    // replace all non-digit characters
    val cleanACN = acn.replaceAll("\\D", "")

    // must be 9 characters
    if (cleanACN.length == 9) {
      val weightFactor = List(8, 7, 6, 5, 4, 3, 2, 1)
      val (number, checksum) = cleanACN.splitAt(8)

      // calculate sum
      val sum = 10 - number.zipWithIndex.foldLeft(0) {
        case (sum, (digit, index)) => {
          sum + (weightFactor(index) * digit.toString.toInt)
        }
      } % 10 
      
      // validate sum
      if (sum == 10) checksum.toInt == 0 else checksum.toInt == sum
    } else {
      false
    }
  }   

}