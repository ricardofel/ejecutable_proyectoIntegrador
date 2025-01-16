//AUTOR: GRUPO CHIMBORAZO

import kantan.csv.ops.{toCsvInputOps, toCsvOutputOps}
import kantan.csv.rfc
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import java.io.File
import play.api.libs.json._

object limpieza extends App {
  //RUTA DEL ARCHIVO DE ENTRADA:
  val ruta_entrada = "data/pi_movies_small.csv"
  //RUTA DEL ARCHIVO DE SALIDA:
  val ruta_salida = "data/pi_movies_small_limpio.csv"
  //VARIABLE QUE VA A ALMACENAR LA DATA LIMPIA:
  var data_limpia: List[movie_class] = null

  // LEER EL .CSV CON DELIMITADOR ';'
  val dataCSV_original = new File(ruta_entrada).readCsv[List, movie_class](rfc.withHeader.withCellSeparator(';'))

  // FILAS VALIDAS
  val filas_validas = dataCSV_original.collect { case Right(movie) => movie}
  //FILAS INVALIDAS
  val filas_invalidas = dataCSV_original.collect{ case Left(error) => error}

  //IMPRIMIR INFORME DE FILAS
  println("FILAS VALIDAS: " + filas_validas.length)
  println("FILAS INVALIDAS: " + filas_invalidas.length)

  // FUNCION PARA LIMPIEZA DE JSON
  def cleanJson(json: String): String = {
    json
      .trim
      .replaceAll("'", "\"") // CAMBIA COMILLAS SIMPLES POR DOBLES
      .replaceAll("None", "null") // CAMBIA NONE POR NULL
      .replaceAll("True", "true") // NORMALIZAR BOOLEANO
      .replaceAll("False", "false") //NORMALIZAR BOOLEANO
      .replaceAll("""\\""", "") // ELIMINA ESPACIOS
  }

  data_limpia = filas_validas.map { movie =>
    //LIMPIAR ADULT
    val adult_limpio = movie.adult.toString.toLowerCase match {
      case "true" | "t" | "1" => true
      case "false" | "f" | "0" | "" => false
      case _ => false
    }

    //LIMPIAR COLUMNA JSON
    //LIMPIAR BELONGS_TO_COLLECTION
    val belongs_to_collection = Json.parse(Option(movie.belongs_to_collection)
      .filter(_.trim.nonEmpty).map(cleanJson).getOrElse("{}")).toString()


    movie.copy(
      adult = adult_limpio,
      belongs_to_collection = belongs_to_collection,
    )
  }

  // LEER EL .CSV CON DELIMITADOR ';'
  new File(ruta_salida).writeCsv(data_limpia, rfc.withHeader(
      "adult", "belongs_to_collection", "budget", "genres", "homepage", "id",
      "imdb_id", "original_language", "original_title", "overview", "popularity",
      "poster_path", "production_companies", "production_countries", "release_date",
      "revenue", "run_time", "spoken_language", "status", "tagline", "title", "video",
      "vote_average", "vote_count", "keywords", "cast", "crew", "ratings"
    ).withCellSeparator(';')
  )
  println(s"ARCHIVO LIMPIO ESCRITO EXITOSAMENTE: $ruta_salida")
}