package ground.generator

import ground.Logging
import ground.spark.SparkUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class PopulateGrid (val gridCSV : String) extends Logging{
  import SparkUtils._

  implicit val session = SparkUtils.sparkSession()
  //read netting set trade mapping csv
  def readMappingCSV = {

    loadCSV(session, "/home/imran/dev/git/spark-playground/src/main/resources/nettingset-trademapping.csv")
  }

  def trades(df : DataFrame) : Dataset[Row] = {
    df.select(df("trade_id"))
  }


  def whatever(trades: DataFrame, scenario : Int, tenor : Int) : DataFrame = {
    //val gridRDD =
    //trades.rdd.fold()
   // trades.rdd.flatMap(trade => GridGenerator(scenario, tenor, 100).generate(session, trade.getAs[String](0)))

  }

  //for each trade generate grid
  def gridForEachTrade(trade : String)  = {
    val gridRDD = GridGenerator(100, 55, 100).generate(session, trade)
   // gridRDD.first()
    val schema = new StructType().add(StructField("trades", StringType)).add(StructField("exposure_grid", ArrayType(FloatType), true))
    session.createDataFrame(gridRDD, schema)
  }

  //write the grid against each trade into output csv
  def writeTradeGridCSV(trade: String, grid : Array[Array[Float]]) : Unit = ???


  /*def writeAllGrids = {
    import session.implicits._

    val df = readMappingCSV
    val tradesDF = trades(df)
    tradesDF.withColumn("exposure_grid", gridForEachTrade(tradesDF.col("trade_id")))
    trades(df).foreach(t => writeTradeGridCSV(t.getAs[String](0),gridForEachTrade(t.getAs[String](0))))
  }*/
}


object PopulateGrid {

  def apply(path : String): PopulateGrid = new PopulateGrid(path)
}


object poprun extends Logging {
  def main(args: Array[String]): Unit = {
    val pg = PopulateGrid("/home/imran/dev/git/spark-playground/src/main/resources/out.csv")
    val df = pg.readMappingCSV

    val trades = pg.trades(df)
    val grids = pg.gridForEachTrade("")

    //grids.printSchema()
    log.info("", grids.show(2))
    //println(grids)
    //pg.writeAllGrids
    //trades.show(10)
  }
}