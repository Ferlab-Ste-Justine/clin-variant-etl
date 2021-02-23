package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.testutils.WithSparkSession
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

case class Test(uid: String, oid: String,
                createdOn: Timestamp, updatedOn: Timestamp,
                data: Long, chromosome: String = "1", start: Long = 666)

class DeltaUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  "upsert" should "update existing data and insert new data" in {

    spark.sql("DROP TABLE IF EXISTS testtable")

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)

    val existing: DataFrame = Seq(
      Test("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      Test("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    ).toDF

    DeltaUtils.writeOnce(existing, "testtable", "spark-warehouse")

    val updates: Seq[Test] = Seq(
      Test("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      Test("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      Test("aaa", "aaa", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    )
    val updatedDF = updates.toDF

    val expectedResult: Seq[Test] = updates

    DeltaUtils.upsert(
      "spark-warehouse",
      "testtable",
      updatedDF,
      "uid"
    )

    DeltaTable
      .forName("testtable")
      .toDF.as[Test].collect() should contain allElementsOf expectedResult

  }

  "scd1" should "update existing data and insert new data" in {

    spark.sql("DROP TABLE IF EXISTS testtable")

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)

    val existing: DataFrame = Seq(
      Test("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      Test("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    ).toDF

    DeltaUtils.writeOnce(existing, "testtable", "spark-warehouse")

    val updates: DataFrame = Seq(
      Test("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      Test("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      Test("aaa", "aaa", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    ).toDF

    val expectedResult: Seq[Test] = Seq(
      Test("a", "b", Timestamp.valueOf(day1), Timestamp.valueOf(day2), 2),   //updated only will be updated
      Test("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2), //will be inserted
      Test("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)//will stay the same
    )

    DeltaUtils.scd1(
      "spark-warehouse",
      "testtable",
      updates,
      "uid",
      "oid",
      "createdOn",
      "updatedOn"
    )

    DeltaTable
      .forName("testtable")
      .toDF.as[Test].collect() should contain allElementsOf expectedResult

  }

}
