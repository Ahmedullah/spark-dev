#!/usr/bin/env bash
 
if [[ $# -eq 7 ]]; then
	echo "***** Submitting job to Spark"
	echo "***** Master -> "$1
	echo "***** Python code location -> "$2

	echo "***** Deleting Spark output folder -> /"$7

	hdfs dfs -rm -R $7

	${SPARK_HOME}"/bin/spark-submit" --master $1 $2 $3 $4 $5 $6 $7
else
	echo "Usage: key-summary.sh master python_code input_file_url no_of_fields key_field_index value_field_index output_folder"
fi

