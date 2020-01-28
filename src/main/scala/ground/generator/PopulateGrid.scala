package ground.generator

import ground.Logging
import ground.spark.SparkUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class PopulateGrid (val gridCSV : String) extends Logging with Serializable {
  import SparkUtils._

  implicit val session = SparkUtils.sparkSession()
  //read netting set trade mapping csv
  def readMappingCSV = {

    loadCSV(session, "/home/imran/dev/git/spark-playground/src/main/resources/nettingset-trademapping.csv")
  }

  def trades(df : DataFrame) : Dataset[Row] = {
    df.select(df("trade_id"))
  }


  def gridForAllTrades(trades: DataFrame, scenario : Int, tenor : Int) : DataFrame = {
    //val gridRDD =
    //trades.rdd.fold()
    val alltradeGrids = trades.rdd.map(trade => gridForEachTrade(trade.getAs[String](0)))
    val flattenedGrids = alltradeGrids.fold(session.emptyDataFrame.rdd)((zero, rdd) => zero.union(rdd))
    val gridSchema = new StructType().add(StructField("trades", StringType)).add(StructField("exposure_grid", ArrayType(FloatType), true))
    session.createDataFrame(flattenedGrids, gridSchema)
  }

  //for each trade generate grid
  def gridForEachTrade(trade : String)  = {
    val sc  = session.sparkContext
    val gridRDD = GridGenerator(100, 55, 100).generate(session, trade)
    val gridSchema = new StructType().add(StructField("trades", StringType)).add(StructField("exposure_grid", ArrayType(FloatType), true))
    session.createDataFrame(gridRDD, gridSchema)
    gridRDD
  }

  //write the grid against each trade into output csv
 // def writeTradeGridCSV(trade: String, grid : Array[Array[Float]]) : Unit = ???


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
 //   val grids = pg.gridForEachTrade("")
      val grids = pg.gridForAllTrades(trades, 10, 20)
    grids.show(10)
    //grids.printSchema()
    //println(grids)
    //pg.writeAllGrids
    //trades.show(10)
  }
}