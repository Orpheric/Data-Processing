package institut.superieur.management

import org.apache.spark.sql.functions.{avg, sum, udf, when,count}
import org.apache.spark.sql.{DataFrame, SparkSession}
import services.OrphericAzainon

import scala.collection.immutable.Seq

object SparkFinalProject {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Orpheric Spark Final Project").getOrCreate()
    spark.sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "-1")
    /*
     Description des colonnes:
    |______________________________________________________________________________________________________________|
    |Variable 	    | Definition 	                                | Key                                            |
    |---------------|---------------------------------------------|------------------------------------------------|
    |survival 	    | Survival 	                                  | 0 = No, 1 = Yes                                |
    |pclass 	      | Ticket class 	                              | 1 = 1st, 2 = 2nd, 3 = 3rd                      |
    |Sex 	          | Sex                                         |                                                |
    |Age 	          | Age in years                                |                                                |
    |sibsp 	        | # of siblings / spouses aboard the Titanic  |                                                |
    |parch 	        | # of parents / children aboard the Titanic  |                                                |
    |ticket 	      | Ticket number                               |                                                |
    |fare 	        | Passenger fare                              |                                                |
    |cabin 	        | Cabin number                                |                                                |
    |embarked 	    | Port of Embarkation 	                      | C = Cherbourg, Q = Queenstown, S = Southampton |
    |---------------|---------------------------------------------|------------------------------------------------|
     */

    /*
    Question 1:
    Créer un Objet Scala intitulé PrenomNom par exemple "IbrahimaFall"
     */

    /*
    Question 2:
    Télécharger les fichiers titanic.csv et country.csv
     puis mettez les dans un nouveau dossier "data" que vous créerez dans le répertoire src.
    */

    /*
    Question 3:
    Dans votre Objet Scala, créer une fonction qui permet de lire un fichier quelconque
    Tips: Cette fonction doit pouvoir lire un fichier de n'importe quel format (csv, json, parquet ...),
    elle doit pouvoir prendre en charge des CSV avec ou sans header, la pluralité des délimiteur ainsi que le chemin du fichier.
     */


    /*
    Question 4:
    Utiliser la fonction readFile pour lire le fichier titanic.csv puis l'afficher
     */
    val titanicDF: DataFrame = OrphericAzainon.readFile(spark, "csv","true", ",", "src/data/titanic.csv")
    titanicDF.show()
    /*
    Question 4':
    Utiliser la fonction readFile pour lire le fichier country.csv puis l'afficher
    Warning: Vérifier le délimiteur de ce fichier.
     */
    val countryTitanic: DataFrame = OrphericAzainon.readFile(spark, "csv","true", ";", "src/data/country.csv")
    countryTitanic.show()

    /*
    Question 5:
    En utilisant la fonction printSchema constatez, le schéma inféré par Spark.
     */
    titanicDF.printSchema()

    /*
    Question 6:
    Importez les methodes spark suivantes: col, lit, when se trouvant dans org.apache.sql.functions
     */
    import org.apache.spark.sql.functions.{col, lit}

    /*
    Question 7:
    Sélectionnez les colonnes PassengerId, Name, Pclass, Cabin, Sex, Age, Survived, Parch et Embarked,
     */
    val titanicDF2: DataFrame = titanicDF.select("PassengerId", "Name", "Pclass", "Cabin", "Sex", "Age", "Survived", "Parch", "Embarked")

    /*
    Question 8:
    Renommer la colonne Name en FullName
     */
    val titanicDF3: DataFrame = titanicDF2.withColumnRenamed("Name", "FullName")

    /*
    Question 9:
    Donnez le nombre de femme qui ont survécu.
    Tips: Appliquer un filtre sur les colonnes Sex et Survived
     */
    val survivedFemale: DataFrame = titanicDF3.where("Sex =='female'").where("Survived == 1")
    //val numberSurvivedFemale = survivedFemale.count()
    //println(s"Le nombre de femmes qui ont survécu sont: $numberSurvivedFemale")

    /*
    Question 10:
    Ajouter une nouvelle colonne sexletter contenant la première lettre de la colonne Sex
    Tips: Utiliser la fonction substring(str: Column, pos: Int, len: Int)
     */
    val getFirstLetter = (sexe: String) => {
      sexe.substring(0,1)
    }
    val getFirstLetterMethod = udf(getFirstLetter)
    val titanicDF4: DataFrame = titanicDF3.withColumn("sexletter", getFirstLetterMethod(col("Sex")))

    /*
    Question 11:
    Aficher les valeurs distinctes de la colonne Pclass (vous avez 2 possibilités)
     */
    val distinctPclass: DataFrame = titanicDF4.select("Pclass").distinct()
    distinctPclass.show()

    /*
    Question 12:
    Supprimer la colonne Parch et embarked
     */
    val titanicDF5: DataFrame = titanicDF4.drop("Parch", "Embarked")

    /*
    Question 13:
    Ajouter une nouvelle colonne isOldEnaugh dans titanic, contenant 1 si le passager
    a plus de 18 ans et 0 dans le cas contraire.
    Tips: Utiliser la withColumn avec les fonction when-otherwise (rf au cours)
     */
    val titanicDF6: DataFrame = titanicDF5.withColumn("isOldEnaugh", when(col("Age") > 18, lit(1)).otherwise(lit(0)))

    /*
    Question 14:
    La colonne Cabin contient beaucoup de valeurs null. Documentez vous sur l'utilisation
    de la methode na.fill puis remplacer les valeurs null dans Cabin par 0
     */
    val titanicDF7: DataFrame = titanicDF6.na.fill("0", Seq("Cabin"))

    /*
    Question 15:
    Calculez le nombre de survivants et de morts ainsi que la moyenne d'age par Sex.
    Est ce que l'age ou le sexe ont un impacte sur la survie?
    Tips: Faites un groupBy sur la colonne Sex puis aggrégez sur la colonne Survived. (ref au cours)
     */
    val survivedBySex = titanicDF7.groupBy("Sex","Survived").agg(avg("Age"))
    // Non l'age ou le sexe n'a pas d'impact sur la survie

    /*
    Question 16:
    Calculez le nombre de passagers par Sex qui n'ont pas survécus et ont moins de 18 ans.
    Tips: Faites un groupBy sur les colonnes Sex et isOldEnaugh puis aggrégez sur la colonne Survived. (ref au cours)
     */
    val survivedYoungBySex: DataFrame = titanicDF7.where("Survived == 0").where("isOldEnaugh == 1").groupBy("Sex","isOldEnaugh").agg(count("Survived"))

    /*
    Question 17:
    Faites une jointure de type 'inner' entre countryTitanic et titanicDF7 en utilisant
    la clef de jointure 'PassengerId'. (ref au cours)
     */
    val dfFinal: DataFrame = countryTitanic.join(titanicDF7, Seq("PassengerId"), "inner")
    /*
    Question 18:
    Quel est le pays qui a eu le plus survivants et celui qui a eu le plus de morts?
    Tips: Faites un groupBy sur la colonne Country puis aggrégez sur la colonne Survived. (ref au cours)
     */
    val titanicDF8: DataFrame = dfFinal.groupBy("Country").agg(sum("Survived"))


    /*
    Question 19:
    Ecriver le dataframe calculé à la question 17 (dfFinal) dans la table hive ism_m2_2022_examspark.votreNomCmoplet

    Pour cela:
    - Vous devez déployer le projet sur la plateforme en utilisant le script lancement_job.sh que vous modifierez suivant votre projet.
     */

    OrphericAzainon.writeDataframeinHive(dfFinal)
  }


}
