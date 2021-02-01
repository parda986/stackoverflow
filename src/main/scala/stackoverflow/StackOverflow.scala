package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    val vectorsCount = vectors.count()
    assert(vectorsCount == 2121822, "Incorrect number of vectors: " + vectors.count())
    println(vectorsCount)
    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
    println("Esto es una pruebaauuaaaa" +
      "gg7gg" +
      "gg425ggg4444  44" +
      "rrr")
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 1
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together
    * QUESTIONS: obtiene el id de la pregunta y la propia pregunta
    * ANSWERS: obtiene el id de la pregunta y la propia respuesta
    * Hacemos un JOIN para quedarnos con un RDD de registros (key,(q, a)). Esto lo que hace es quedarse con todas las
    * respuestas que tienen pregunta.
    * Agrupamos por clave todas las respuestas, nos quedamos con un PairRDD (QIZ, coleccion de tipo (pregunta, respuesta))
    */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings.filter(_.postingType == 1).map(q => (q.id, q))
    val answers = postings.filter(_.postingType == 2).map(a => (a.parentId.get, a))
    questions.join(answers).groupByKey()
//    val questions = postings.filter(_.postingType == 1).map(q => q.id -> q)
//    val answers = postings.filter(p => p.postingType == 2 && p.parentId.nonEmpty).map(a => a.parentId.get -> a)
//    questions.join(answers).groupByKey
  }

  /** Compute the maximum score for each posting*/
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {
    // Recorre el array de respuestas de cada pregunta y devuelve la mejor valorada
    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }
    //toma el PairRDD (QIZ, coleccion de tipo (pregunta, respuesta)) param y aplica un map a los elementos obteniendo pares:
    //1er elemento: ID de la pregunta; 2do elemento: la puntuacion mas alta de las respuestas convertidas a Array
    grouped.map(v => (v._2.head._1,  answerHighScore(v._2.map(_._2).toArray)))
  }

  /** Compute the vectors for the kmeans
    * -Si el tag esta vacio o los lenguajes vacios devuelve un None
    * -Si coincide que el primer lenguaje de la lista es el del tag ==> index = 0
    * Si no llama a la funcion sobre la cola recursivamente y lo guarda en tmp
    * -Si tmp es None se devuelve None
    * -Si contiene un indice se devuelve el indice + 1
    *
    * Convierte scored RDD[(Question, HighScore)])
    * en elementos vectors RDD[(LangIndex, HighScore)]
    * aplicando la funcion firstLangInTag sobre la tag de cada pregunta con los lenguajes provistos langs
    * Obtenemos vectores de pares (indice lenguaje del post(-1 si no esta en la lista), puntuacion maxima de las respuestas del post)
    * Filtramos para quitar los lenguajes de los post que no estaban en la lista de lenguajes(indice < 0)
    * llamamos a cache() para disponer de esta operacion en memoria
    */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /**
      * Return optional index of first language that occurs in `tags`
      */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }
    scored.map(score =>
      (firstLangInTag(score._1.tags, langs).getOrElse(-1) * langSpread, score._2))
      .filter(_._1 >= 0)
      .cache()
  }

  /**
    * Sample the vectors
    * @param vectors RDD con los lenguajes de los post y las mas altas puntuaciones
    * @return un Array con las posiciones de los vectores seleccionados aleatoriamente
    */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    /**
      *
      * -Va cargando el Array
      * @param lang para cada lenguaje en vectors
      * @param iter iterador sobre la coleccion de HighScore del language
      * @param size nº de clusters/agrupaciones por lenguage
      * @return
      */
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      //-Crea un array de Int pora las muestras de tamaño = nº clusters por lenguaje
      val res = new Array[Int](size)
      //-Crea un random con el indice del lenguaje
      val rnd = new util.Random(lang)
      //-Va cargando el Array con vectores aleatorios
      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    /**
      * RESULTADO DEVUELTO POR sampleVectors:
      * - Si la distancia es menos de 500 tomamos una muestra aleatoria de 45 vectores
      */
    val res =
      if (langSpread < 500) {
        // sample the space regardless of the language; seed = 42 por ejemplo, para obtener siempre mismos resultados
        // en PROD esto se reemplaza por un verdadero random generado
        vectors.takeSample(false, kmeansKernels, 42)
        /**
          * -Si no, muestrear el espacio uniformemente de cada partición de idioma:
          * agrupa los vectores por clave y hace un flatmap: para cada par (langIndex, Iterable[HighScore]):
          * llama a reservoirSampling(langIndex, iterador sobre los vectores del lenguaje, nº clusters por lenguaje)
          */
      } else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }

  /**
    * Main kmeans computation
    * Toma los vectores aleatorios de los langIndex y sus HighScore
    * @param means promedios aleatorios que ha obtenido el sampleVectors, 3 por lenguaje (45)
    * @param vectors todos los vectores inciales 2.282.145 o los que sean
    * @param iter para controlar el numero de iteraciones empezando en 1
    * @param debug true para ver el log
    * @return
    */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    val newMeans = means.clone() // you need to compute newMeans
    //Clona los vectores seleccionados aleatoriamente
    //Coge los vectores de todas las puntuaciones y para cada uno de ellos encuentra el promedio mas cercano
    //lo devuelve en pares (indice del promedio mas cercano 0-45, Vector[(LangIndex, HighScore)])
    //los agrupa por clave obteniendo pares (promedio, Iterable[Vector[(LangIndex, HighScore)]]
    //aplica averageVectors a la coleccion de vectores Iterable[Vector[(LangIndex, HighScore)]] de cada promedio
    //obteniendo pares (promedio, (avgLangIndex, avgHighScore)) y se procesa el Action con collect()
    val newMeansWithIndex = vectors.map(vector => findClosest(vector, means) -> vector).groupByKey.mapValues(averageVectors).collect()
    //Para cada uno de los 45 items de newMeansWithIndex actualizamos los de newMeans / nuevos promedios
    //con los avgLangIndex y los avgHighScore de los vectores del cluster anterior
    newMeansWithIndex.foreach(item => newMeans.update(item._1, item._2))

    // Calcula la distancia euclidea entre los means iniciales y los newMeans
    val distance = euclideanDistance(means, newMeans)
    //Imprime cada iteracion hasta completar maximo de iteraciones o hasta converger
    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }
    // Si los centroides ya no cambian de posicion devolvemos los nuevos promedios
    if (converged(distance)) {
      newMeans
      //Si todavia no convergen y no hemos superado maxIterations volvemos a llamar a kmeans aumentando el contador
    } else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      // Si hemos llegado a maxIterations imprimimos el mensaje y devolvemos los grupos alcanzados
      // newMeans : Array[(LangIndex, HighScore)]
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    // Calcula lo que se han movido los centroides en cuanto a LangIndex
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    // Calcula lo que se han movido los centroides en cuanto a HighScore
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    // Suma las partes
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    //Mientras no hayamos recorrido los means (45)
    while(idx < a1.length) {
      //Acumulamos la distancia euclidea entre los vectores correspondientes de cada array de 45 length en la posicion idx
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point de cada uno de los vectores[(LangIndex, HighScore)]
    * val newMeansWithIndex = vectors.map(vector => findClosest(vector, means) -> vector).groupByKey.mapValues(averageVectors).collect()
    * newMeansWithIndex.foreach(item => newMeans.update(item._1, item._2))
    * - */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors agrupados a un centroide o en un cluster
    * Iterable[Vector[(LangIndex, HighScore)]]
    * Recorre los vectores agrupados en cada promedio para obtener una media
    * de las posiciones de todos ellos, y asignarla como nuevo centroide
    */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      // Suma los indices del lenguaje
      comp1 += item._1
      // Suma las puntuaciones de los lenguajes y aumenta el contador
      comp2 += item._2
      count += 1
    }
    // Devuelve un par compuesto por:
    // -Media de los indices de lenguaje,
    // -Media de las puntuaciones del cluster,
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  def medianVectors(s: Seq[Double]) = {
    val (lower, upper) =
      s.sortWith(_ < _).
        splitAt(s.size / 2)
    if (s.size % 2 == 0)
      (lower.last + upper.head) / 2.0
    else upper.head
  }

  //
  //
  //  Displaying results:
  //
  // Finalmente agrupa los post en base a los promedios obtenidos
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    // a cada vector lo convierte en un par (indice del promedio(0-45), vector[(LangIndex, HighScore)])
    val closest = vectors.map(p => (findClosest(p, means), p))
    // agrupa los vectores por indice del promedio
    val closestGrouped = closest.groupByKey()
    // Obtiene las estadisticas. Para cada coleccion Iterable[(LangIndex, HighScore)] de cada promedio
    // la agrupa por langIndex y a eso le aplica un map para obetener pares (langIndex, vectors.length)
    // y finalmente obtener el lenguaje mas popular en el cluster
    val median = closestGrouped.mapValues { vs =>
      val langIndex: Int      =
        vs.groupBy(xs => xs._1)
          .map(xs => (xs._1, xs._2.size))
          .maxBy(xs => xs._1)._1 / langSpread
      val langLabel: String   = langs(langIndex) // most common language in the cluster
      val clusterSize: Int    = vs.size // tamaño del cluster
      val langPercent: Double = vs.map( { case (v1, v2) => v1 })
        .filter(v1 => v1 == langIndex * langSpread).size * 100 / clusterSize // percent of the questions in the most common language
      val medianScore: Int    = medianVectors(vs.map(_._2.toDouble).toSeq).toInt

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
