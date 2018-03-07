import org.apache.spark._
import spark.implicits._

 val books = spark.read.json("examples/doutorado/books2publication/books-800mil.json")

 val booksoutcome = books.select("booktitle", "chapter.nbPages")

 val totalPages = booksoutcome.map(x => (x.getString(0), x.getSeq(1).map((x: Long) => x).sum))

 val finaloutcome = totalPages.map (x => "{" + """"title"""" + ":" + """"""" + x._1 + """"""" + ", " + """"nbPages"""" + ":" + x._2 + "}")

finaloutcome.rdd.saveAsTextFile("examples/doutorado/books2publication/outcome")

System.exit(0)
