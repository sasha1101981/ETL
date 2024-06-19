/*
chcp 65001 && spark-shell -i \spark\d1.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis()
if(1==1){
		var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("/spark/d1.xlsx")
		df1.show()
		
		df1.filter(df1("Employee_ID").isNotNull).select("Employee_ID","Name","Job_Code","Job","City_code","Home_city")
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tabr")
        .mode("overwrite").save()

		import java.sql._;
		def sqlexecute(sql: String) = {
			var conn: Connection = null;
			var stmt: Statement = null;
			try {
				Class.forName("com.mysql.cj.jdbc.Driver");
				conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?user=root&password=root");
				stmt = conn.createStatement();
				stmt.executeUpdate(sql);
				println(sql+" complete");
			} catch {
				case e: Exception => println ("exception caught" + e);
			}
		}
		sqlexecute("drop table spark.tabr1")
		sqlexecute("drop table spark.tabr2")
		sqlexecute("drop table spark.tabr3")
		sqlexecute("drop table spark.tabr4")
		sqlexecute("""
		CREATE TABLE `tabr1` (
			`City_code` INT(10) NOT NULL,
			`Home_city` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			PRIMARY KEY (`City_code`) USING BTREE
		)
		COMMENT='Успешно!'
		COLLATE='utf8mb4_0900_ai_ci'
		ENGINE=InnoDB
		""")
		sqlexecute("""
		CREATE TABLE `tabr2` (
			`Job_Code` VARCHAR(50) NOT NULL COLLATE 'utf8mb4_0900_ai_ci',
			`Job` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			PRIMARY KEY (`Job_Code`) USING BTREE
		)
		COMMENT='Успешно!'
		COLLATE='utf8mb4_0900_ai_ci'
		ENGINE=InnoDB
		;
		""")
		sqlexecute("""
		CREATE TABLE `tabr3` (
			`Employee_ID` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			`Name` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			`Job_Code` VARCHAR(50) NOT NULL COLLATE 'utf8mb4_0900_ai_ci',
			INDEX `Employee_ID` (`Employee_ID`, `Name`) USING BTREE,
			INDEX `Job_Code` (`Job_Code`) USING BTREE
		)
		COMMENT='Успешно!'
		COLLATE='utf8mb4_0900_ai_ci'
		ENGINE=InnoDB
		;
		""")
		sqlexecute("""
		CREATE TABLE `tabr4` (
			`Employee_ID` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			`Name` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
			`City_code` INT(10) NOT NULL,
			INDEX `Employee_ID` (`Employee_ID`, `Name`) USING BTREE,
			INDEX `City_code` (`City_code`) USING BTREE
		)
		COMMENT='Успешно!'
		COLLATE='utf8mb4_0900_ai_ci'
		ENGINE=InnoDB
		;
		""")
		df1.select("City_code","Home_city").distinct()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")
		.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tabr1")
        .mode("append").save()
		df1.select("Job_Code","Job").distinct()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")
		.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tabr2")
		.mode("append").save()
		df1.select("Employee_ID","Name","Job_Code").distinct()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")
		.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tabr3")
		.mode("append").save()
		df1.select("Employee_ID","Name","City_code").distinct()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")
		.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tabr4")
        .mode("append").save()
		
	println("task 1")
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)