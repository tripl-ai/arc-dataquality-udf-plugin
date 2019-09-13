package ai.tripl.arc.plugins.udf

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession

import ai.tripl.arc.util.log.logger.Logger
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.util.Utils

import com.google.i18n.phonenumbers.PhoneNumberUtil
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat

class DataValidation extends ai.tripl.arc.plugins.UDFPlugin {

  val version = Utils.getFrameworkVersion

  // one udf plugin can register multiple user defined functions
  override def register()(implicit spark: SparkSession, logger: Logger, arcContext: ARCContext) = {

    // register custom UDFs via sqlContext.udf.register("funcName", func )
    spark.sqlContext.udf.register("is_valid_phonenumber", DataValidationPlugin.isValidPhoneNumber _ )
    spark.sqlContext.udf.register("format_phonenumber", DataValidationPlugin.formatPhoneNumber _ )
    spark.sqlContext.udf.register("is_valid_abn", DataValidationPlugin.isValidABN _ )

  }
}

object DataValidationPlugin {

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
    phoneUtil.format(parsed, PhoneNumberFormat.E164)
  }

  // validates whether an ABN passes the inbuilt checksum function
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

}