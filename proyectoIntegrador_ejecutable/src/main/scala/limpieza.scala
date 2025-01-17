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
    val adult_limpio = movie.adult.toString.trim.toLowerCase match {
      case "true" | "t" | "1" => true
      case "false" | "f" | "0" | "" => false
      case _ => false
    }

    //LIMPIAR VIDEO
    val video_limpio = movie.video.toString.trim.toLowerCase match {
      case "true" | "t" | "1" => true
      case "false" | "f" | "0" | "" => false
      case _ => false
    }

    // LIMPIAR BELONGS_TO_COLLECTION
    val belongsToCollection_limpio = if (movie.belongs_to_collection == null || movie.belongs_to_collection.trim.isEmpty) {
      "{}"
    } else {
      val cadena_original = movie.belongs_to_collection.trim
      if ((!cadena_original.endsWith("}"))&&(!cadena_original.startsWith("{"))) {
          "{" + cleanJson(cadena_original) + "}"
      } else if (!cadena_original.endsWith("}")) {
          cleanJson(cadena_original) + "}"
      } else if (!cadena_original.startsWith("{")) {
          "{" + cleanJson(cadena_original)
      } else {
          cleanJson(cadena_original)
      }
    }

    // LIMPIAR GENRES
    val genres_limpio = if (movie.genres == null || movie.genres.trim.isEmpty) {
      "[]"
    } else {
      val cadena_original = movie.genres.trim
      if ((!cadena_original.endsWith("]"))&&(!cadena_original.startsWith("["))) {
        "[" + cleanJson(cadena_original) + "]"
      } else if (!cadena_original.endsWith("]")) {
        cleanJson(cadena_original) + "]"
      } else if (!cadena_original.startsWith("[")) {
        "[" + cleanJson(cadena_original)
      } else {
        cleanJson(cadena_original)
      }
    }

    // LIMPIAR PRODUCTION COMPANIES
    val production_companies_limpio = if (movie.production_companies == null || movie.production_companies.trim.isEmpty) {
      "[]"
    } else {
      val cadena_original = movie.production_companies.trim
      if ((!cadena_original.endsWith("]"))&&(!cadena_original.startsWith("["))) {
        "[" + cleanJson(cadena_original) + "]"
      } else if (!cadena_original.endsWith("]")) {
        cleanJson(cadena_original) + "]"
      } else if (!cadena_original.startsWith("[")) {
        "[" + cleanJson(cadena_original)
      } else {
        cleanJson(cadena_original)
      }
    }

    // LIMPIAR PRODUCTION COUNTRIES
    val production_countries_limpio = if (movie.production_countries == null || movie.production_countries.trim.isEmpty) {
      "[]"
    } else {
      val cadena_original = movie.production_countries.trim
      if ((!cadena_original.endsWith("]"))&&(!cadena_original.startsWith("["))) {
        "[" + cleanJson(cadena_original) + "]"
      } else if (!cadena_original.endsWith("]")) {
        cleanJson(cadena_original) + "]"
      } else if (!cadena_original.startsWith("[")) {
        "[" + cleanJson(cadena_original)
      } else {
        cleanJson(cadena_original)
      }
    }

    // LIMPIAR SPOKEN_LANGUAGES
    val spoken_languages_limpio = if (movie.spoken_languages == null || movie.spoken_languages.trim.isEmpty) {
      "[]"
    } else {
      val cadena_original = movie.spoken_languages.trim
      if ((!cadena_original.endsWith("]"))&&(!cadena_original.startsWith("["))) {
        "[" + cleanJson(cadena_original) + "]"
      } else if (!cadena_original.endsWith("]")) {
        cleanJson(cadena_original) + "]"
      } else if (!cadena_original.startsWith("[")) {
        "[" + cleanJson(cadena_original)
      } else {
        cleanJson(cadena_original)
      }
    }

    // LIMPIAR KEYWORDS
    val keywords_limpio = if (movie.keywords == null || movie.keywords.trim.isEmpty) {
      "[]"
    } else {
      val cadena_original = movie.keywords.trim
      if ((!cadena_original.endsWith("]"))&&(!cadena_original.startsWith("["))) {
        "[" + cleanJson(cadena_original) + "]"
      } else if (!cadena_original.endsWith("]")) {
        cleanJson(cadena_original) + "]"
      } else if (!cadena_original.startsWith("[")) {
        "[" + cleanJson(cadena_original)
      } else {
        cleanJson(cadena_original)
      }
    }

    // LIMPIAR CAST
    val cast_limpio = if (movie.cast == null || movie.cast.trim.isEmpty) {
      "[]"
    } else {
      val cadena_original = movie.cast.trim
      if ((!cadena_original.endsWith("]"))&&(!cadena_original.startsWith("["))) {
        "[" + cleanJson(cadena_original) + "]"
      } else if (!cadena_original.endsWith("]")) {
        cleanJson(cadena_original) + "]"
      } else if (!cadena_original.startsWith("[")) {
        "[" + cleanJson(cadena_original)
      } else {
        cleanJson(cadena_original)
      }
    }

    // LIMPIAR CREW
    val crew_limpio = if (movie.crew == null || movie.crew.trim.isEmpty) {
      "[]"
    } else {
      val cadena_original = movie.crew.trim
      if ((!cadena_original.endsWith("]"))&&(!cadena_original.startsWith("["))) {
        "[" + cleanJson(cadena_original) + "]"
      } else if (!cadena_original.endsWith("]")) {
        cleanJson(cadena_original) + "]"
      } else if (!cadena_original.startsWith("[")) {
        "[" + cleanJson(cadena_original)
      } else {
        cleanJson(cadena_original)
      }
    }

    // LIMPIAR RATINGS
    val ratings_limpio = if (movie.ratings == null || movie.ratings.trim.isEmpty) {
      "[]"
    } else {
      val cadena_original = movie.ratings.trim
      if ((!cadena_original.endsWith("]"))&&(!cadena_original.startsWith("["))) {
        "[" + cleanJson(cadena_original) + "]"
      } else if (!cadena_original.endsWith("]")) {
        cleanJson(cadena_original) + "]"
      } else if (!cadena_original.startsWith("[")) {
        "[" + cleanJson(cadena_original)
      } else {
        cleanJson(cadena_original)
      }
    }

    movie.copy(
      adult = adult_limpio,
      video = video_limpio,
      belongs_to_collection = belongsToCollection_limpio,
      genres = genres_limpio,
      production_companies = production_companies_limpio,
      production_countries = production_countries_limpio,
      spoken_languages = spoken_languages_limpio,
      keywords = keywords_limpio,
      cast = cast_limpio,
      crew = crew_limpio,
      ratings = ratings_limpio
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