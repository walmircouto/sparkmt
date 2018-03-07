import org.apache.spark._
import spark.implicits._
import java.io._

val classes = spark.read.json("examples/doutorado/class2relational/Class2milhoes.json")

val datatypes = classes.select("id", "datatype_name").filter($"datatype_name" =!= "null")

// Pegar o ID do tipo Integer, e converter em uma variável Integer para ser usada na formação do JSON de saída
val DataTypeID = datatypes.select($"id").filter($"datatype_name" === "Integer").head().get(0)
val strDataTypeID = DataTypeID.toString
var intDataTypeID = strDataTypeID.toInt

val classesFull = classes.select("id", "class_name", "attributes").filter($"class_name" =!= "null")

// Pegar o valor do maior ID para poder construir os IDs dos registros que vão nascer devido aos atributos multivalorados
val maxID = classes.select(max("id")).as("maxID").head().get(0)
val strMaxID = maxID.toString
var intMaxID = strMaxID.toInt

// Aplica a função MAP para transformar o conteúdo do DataFrame no formato JSON
val JsonDataTypes = datatypes.map(x => "{" + """"id"""" + ":" + x.getLong(0) + ", " + """"type_name"""" + ":" + """"""" + x.getString(1) + """"""" + "}")

// DataFrame que vai guardar as informacoes das classes com todos os atributos separadamente - para facilitar a verificação dos atributos separadamente
val dfClassesFullFinal = classesFull.withColumn("attributes", explode($"attributes")).select($"id", $"class_name", $"attributes".getItem("attr_name").alias("attr_name"), $"attributes".getItem("multiValued").alias("multiValued"), $"attributes".getItem("type").alias("type"))

// DataFrame que só vai guardar os atributos multivalorados das classes 
val dfClassesWithAttrMultivalued = dfClassesFullFinal.select($"id", $"class_name", $"attr_name", $"multiValued", $"type").filter($"multiValued" === true)

// Aplica a função MAP para transformar o conteúdo do DataFrame no formato JSON
val JsonClassesWithAttrMultivalued = dfClassesWithAttrMultivalued.map(x => { intMaxID += 1; "{" + """"id"""" + ":" + intMaxID + ", " + """"table_name"""" + ":" + """"""" + x.getString(1) + "_" + x.getString(2) + """"""" + ", " + """"cols"""" + ":[{" + """"col_name"""" + ":" + """"""" + x.getString(1) + "Id" + """"""" + ", " + """"type"""" + ":" + intDataTypeID + "}, {" + """"col_name"""" + ":" + """"""" + x.getString(2) + "Id" + """"""" + ", " + """"type"""" + ":" + intDataTypeID + "}]}"})

val JsonDataTypeWithAttrMultivalued  = JsonDataTypes.union(JsonClassesWithAttrMultivalued)


// DataFrame que só vai guardar os atributos que NÃO SÃO multivalorados das classes 
val dfClassesNoAttrMultivalued = dfClassesFullFinal.select($"id", $"class_name", $"attr_name", $"multiValued", $"type").filter($"multiValued" === false)


val JsonClassesNoAttrMultivalued =  dfClassesNoAttrMultivalued.map(x => "{" + """"id"""" + ":" + x.getLong(0) + ", " + """"table_name"""" + ":" + """"""" + x.getString(1) + """"""" + ", " + """"cols"""" + ":[{" + """"col_name"""" + ":" + """"objectId"""" + ", " 
														 					+ """"keyOf"""" + ":" + x.getLong(0) + ", " + """"type"""" + ":" + intDataTypeID + "}, " + "{" + """"col_name"""" + ":" + """"""" + x.getString(2) + """"""" + ", " + """"type"""" + ":" + x.getLong(4) + "}]}" 

																  
																  )

val JsonFinal = JsonClassesNoAttrMultivalued.union(JsonDataTypeWithAttrMultivalued)

JsonFinal.rdd.saveAsTextFile("examples/doutorado/class2relational/outcome")

System.exit(0)
