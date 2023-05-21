spark-submit:
	spark-submit \
        --master spark://spark-master:7077 \
        /app/spark_docker_sample/meucodigo.py \
        ${PARAMETROS_ADICIONAIS}