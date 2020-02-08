package ground.spark


import cats.data.Writer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object writelog {

  implicit class logop(s: String) {
    def ~> [A](rdd : RDD[A]) : Writer[List[String], RDD[A]] = Writer(s :: Nil, rdd)
    def ~~> [A](df : DataFrame) : Writer[List[String], DataFrame] = Writer(s:: Nil, df)
  }

}
