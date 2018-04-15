package cn.abelib.scala

/**
  * Created by ${abel-huang} on 18/4/3.
  */
object NewClass {
  def test(a:Int)(b:Int):Int = {
    a+b
  }

  //curry
  val curry = test(1)_

  def main(args: Array[String]): Unit = {
    testMap
  }


  def testList: Unit ={
    val numbers  = List(1, 2, 3, 4)
    println(numbers.length)
    println(numbers)
  }

  def testTuple: Unit ={
    val t = (1, 2, 3)

    println(t)
    println(t._2)

    val t2 = t->"2"
    println(t2)
  }

  def testSet: Unit ={
    val s = Set(1, 2, 2)
    println(s)
  }

  def testMap: Unit ={
    val map = Map(1->2, "foo"->"bar")
    println(map)
  }

}
