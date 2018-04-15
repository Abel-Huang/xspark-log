package cn.abelib.scala

/**
  * Created by ${abel-huang} on 18/4/11.
  */
object TestSlice {
  def main(args: Array[String]): Unit = {
    val array =  Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val newArray = array.slice(8, array.length).mkString(" ")
   println(newArray)
    println(newArray.length)

  }
}
