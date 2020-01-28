package ground.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {

  /**
    * Creates a SparkSession with Hive enabled
    */
  def sparkSession(): SparkSession = {
    val session = SparkSession.builder()
      .config("spark.master", "local")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    // Import the implicits, unlike in core Spark the implicits are defined
    // on the context.
    import session.implicits._
    session
  }

  def loadCSV(session: SparkSession, path : String) : DataFrame =
    session.read.option("header",true).csv(path)

}
