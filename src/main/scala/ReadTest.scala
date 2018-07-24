import org.apache.spark.sql.SparkSession

case class Reviewer(id: String, name: String, gender: String, age: Long, salary: Double)

object ReadTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Read")
      .master("local[1]")
      .getOrCreate()

    val reviewers = spark.read.json("data/reviewers_small.json")

    reviewers.printSchema()

    reviewers.show(100)

    import spark.implicits._
    import org.apache.spark.sql.functions._


    //selecting...
    reviewers.select("name", "gender").show()

    //Functions on data

    val average = reviewers.agg(avg("salary"))
    val avgSalary = average.as[Double].first()
    println(s"Average Salary is :${avgSalary}")


    //filtering data

    val top10 = reviewers.filter($"salary" > avgSalary).sort($"salary" desc).as[Reviewer].take(10)
    println("TOP 10 reviewers:")
    for (r <- top10){
      println(s"${r.name} ${r.gender} ${r.age} ${r.salary}")
    }


    //grouping

    reviewers.groupBy("age").agg(count("*"))
      .withColumnRenamed("count(1)", "# of people").show(false)




    spark.stop()
  }
}