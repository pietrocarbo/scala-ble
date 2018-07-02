type OptionMap = Map[Symbol, Any]
var map: OptionMap = Map()
//map = map ++ Map('debug -> true)
//map = map ++ Map('parse -> true)
//map = map ++ Map('dpr -> true)
//map = map ++ Map('friends -> true)
//map = map ++ Map('triangles -> true)

work(map.toList)

def work(map: List[(Symbol, Any)]) {
  map match {
//    case ('debug, true) :: tail =>
//      println("found pattern and do something")
//      work(tail)
    case _ =>
      println("do all ops")
  }
}