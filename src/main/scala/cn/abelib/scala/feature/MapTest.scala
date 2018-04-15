package cn.abelib.scala.feature

/**
  * Created by ${abel-huang} on 18/4/8.
  */
object MapTest {
  def main(args: Array[String]): Unit = {
    val v = Vector(1, 2, 3, 4)
    val v2 = v.map(n => n+1)
    v2.map(println)
    println(v2)
    val v3 = v.reduce((sum, n) => sum+n)
    println(v3)
    v.foreach(n => println(n))
    val sum = (1 to 100).reduce((sum, n) => sum + n)
    println(sum)
  }
}
