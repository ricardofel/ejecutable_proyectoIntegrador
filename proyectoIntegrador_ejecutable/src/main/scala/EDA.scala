//AUTOR: GRIPO CHIMBORAZO

import kantan.csv.ops.{toCsvInputOps, toCsvOutputOps}
import kantan.csv.rfc
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import java.io.File
import java.io.File

object EDA extends App {
  val path2DataFile2 = "data/pi_movies_small.csv"

  // Configurar lectura del CSV con delimitador ';'
  val dataSource2 = new File(path2DataFile2)
    .readCsv[List, movie_class](rfc.withHeader.withCellSeparator(';'))


  // Filtrar filas válidas
  val rowsOk = dataSource2.collect {
    case Right(movie) => movie
  }

  val len_rows_ok = rowsOk.length

  val rows_fail = dataSource2.collect{
    case Left(movie) => movie

  }

  val len_rows_notOk = rows_fail.length;
  println("La longitud de las que están bien es: " + len_rows_ok)
  println("La longitud de las que están mal es de: " + len_rows_notOk)

  /*
  Datos estadísticos
   */

  //Duración de las películas
  val minDuration = rowsOk.flatMap(_.runtime).minOption.getOrElse(0.0)
  val maxDuration = rowsOk.flatMap(_.runtime).maxOption.getOrElse(0.0)
  val averageDuration = rowsOk.flatMap(_.runtime).sum / rowsOk.flatMap(_.runtime).length

  //Películas con categoría para adultos
  val adultMovies = rowsOk.filter(_.adult)

  //Lenguajes más usados en las peliculas
  val topLanguages = rowsOk.map(_.original_language).groupBy(identity)
    .map { case (lenguaje, occurrences) =>
      (lenguaje, occurrences.size)
    }.toList.sortBy(-_._2)

  //Peliculas agrupadas por su status actual
  val topStatus = rowsOk.map(_.status).groupBy(identity)
    .map{ case(status, ocurrences) =>
      (status, ocurrences.size)
    }.toList.sortBy(-_._2)

  //Peliculas ordenadas por poularidad
  val topPopularity = rowsOk.map(movie => (movie.title, movie.popularity))
    .sortBy(-_._2)
  //Peliculas ordenadas por revenue
  val topRevenue = rowsOk.map(movie => (movie.title, movie.revenue) )
    .sortBy(-_._2)

  //Peliculas ordenadas por presupuesto
  val topBudget = rowsOk.map(movie => (movie.title, movie.budget))
    .sortBy(-_._2)


  //Imprimir datos recolectados
  println(s"Idioma original más común: ${topLanguages.head}")
  println(s"La pelicula de menor duración tiene: $minDuration minutos")
  println(s"La pelicula de mayor duración tiene: $maxDuration minutos")
  println(s"El promedio de duración de las películas es de: $averageDuration minutos")
  println(s"La cantidad de películas que son de categoría adultos es de: ${adultMovies.length}" )


  topBudget.foreach{ case(title, budget) =>
    println(s"Pelicula: $title, Bugdet (presupuesto): $budget")
  }

  topPopularity.foreach{ case(title, popularity) =>
    println(s"Pelicula: $title, Popularidad: $popularity")
  }

  topRevenue.foreach{ case (title, revenue) =>
    println(s"Pelicula: $title, Revenue: $revenue dolares")
  }
  topLanguages.foreach { case (language, count) =>
    println(s"Lenguaje: $language, Ocurrencias: $count")
  }

  topStatus.foreach { case (status, count) =>
    println(s"Estado de la película: $status, Ocurrencias: $count")
  }
}
