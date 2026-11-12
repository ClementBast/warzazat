FROM apache/spark:3.5.7-python3
USER root
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
RUN pip3 install --no-cache-dir --upgrade pip \
 && pip3 install --no-cache-dir pandas==2.0.3 pyarrow==14.0.2 jupyterlab==4.2.5 \
        matplotlib==3.7.5 seaborn==0.13.2 plotly==5.24.1 pymongo==4.8.0 \
        streamlit==1.39.0 \
 && mkdir -p /home/spark && chown -R spark:spark /home/spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    HOME=/home/spark MPLCONFIGDIR=/tmp/matplotlib PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3
RUN python3 -c "import pyspark, pandas, plotly, seaborn, pymongo; print('pyspark', pyspark.__version__)"
USER spark
WORKDIR /work
