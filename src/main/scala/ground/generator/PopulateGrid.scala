package ground.generator

import cats.data.Writer
import ground.Logging
import ground.spark.SparkUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class PopulateGrid (val gridCSV : String) extends Logging with Serializable {
  import SparkUtils._
  import ground.spark.writelog._

  implicit val session = SparkUtils.sparkSession()
  //read netting set trade mapping csv
  def readMappingCSV = {

    s"loading csv" ~~> loadCSV(session, "/home/imran/dev/git/spark-playground/src/main/resources/nettingset-trademapping.csv")
  }

  def trades(df : DataFrame) : Writer[List[String], Dataset[Row]] = {
    s"selecting trades" ~~> df.select(df("trade_id"))
  }


  def gridForAllTrades(trades: DataFrame, scenario : Int, tenor : Int) : Writer[List[String], DataFrame] = {
    val alltradeGrids = trades.rdd.map(trade => gridForEachTrade(trade.getAs[String](0)))
    val flattenedGrids = alltradeGrids.fold(Seq[Row]())((zero, rdd) => zero.union(rdd))
    //flattenedGrids.toDF("trades", "")
    val gridSchema = new StructType().add(StructField("trades", StringType)).add(StructField("exposure_grid", ArrayType(FloatType), true))
    s"creating grids dataframe for all trades" ~~> session.createDataFrame(session.sparkContext.parallelize(flattenedGrids), gridSchema)
  }

  //for each trade generate grid
  def gridForEachTrade(trade : String)  = {
     GridGenerator(100, 55, 100).generate(trade)
  }
}


object PopulateGrid {
  def apply(path : String): PopulateGrid = new PopulateGrid(path)
}


object poprun extends Logging {

  def main(args: Array[String]): Unit = {
    val pg = PopulateGrid("/home/imran/dev/git/spark-playground/src/main/resources/out.csv")

    import cats.implicits._

    val result = for {
      csvdf <- pg.readMappingCSV
      trades <- pg.trades(csvdf)
      grids <- pg.gridForAllTrades(trades, 10, 20)
    } yield grids

    val (logs, resultDF) = result.run
    logs.foreach(log.info(_))
    resultDF.show(10)
//    val trades = pg.trades(df)
//    val grids = pg.gridForEachTrade("")
//      val grids = pg.gridForAllTrades(trades, 10, 20)
//    grids.show(10)
//    grids.printSchema()
//    println(grids)
//    pg.writeAllGrids
//    trades.show(10)
  }
}