package bio.ferlab.clin.etl.utils

import bio.ferlab.clin.testutils.WithSparkSession
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HadoopFileSystemSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  "list file" should "return a list of file" in {

    val path = this.getClass.getClassLoader.getResource("test_json").getFile
    println(path)

    val files = HadoopFileSystem
      .list(s"file://$path/", recursive = true)

      println(files.length)
  }

  "test" should "test" in {

    val path = this.getClass.getClassLoader.getResource("test_json").getPath
    println(path)

    val p = new Path(path)

    val listStatus = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
      .globStatus(new org.apache.hadoop.fs.Path(path))

    for (urlStatus <- listStatus) {
      println("urlStatus get Path:" + urlStatus.getPath())

    }
  }

}
