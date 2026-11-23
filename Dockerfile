FROM apache/spark:3.5.3

USER root
# L'image Spark livre Python 3.8. NumPy 2.1 et ce projet demandent 3.12.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates xz-utils \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh \
    && uv python install 3.12 \
    && uv venv /opt/venv --python 3.12 --seed \
    && /opt/venv/bin/python -c "import sys; assert sys.version_info[:2] >= (3, 12), sys.version"

ENV PATH=/opt/venv/bin:$PATH \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    PYTHONPATH=/work/src:/work/third_party \
    SPARK_JARS_PRELOADED=1 \
    SPARK_NO_DAEMONIZE=true

RUN /opt/venv/bin/pip install --no-cache-dir \
        pyspark==3.5.3 \
        numpy==2.1.3 \
        setuptools==75.6.0 \
        delta-spark==3.2.0 \
        kafka-python==2.2.11 \
        jupyterlab==4.3.5

COPY docker/prefetch.py /tmp/prefetch.py
RUN /opt/spark/bin/spark-submit --master local[1] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0,graphframes:graphframes:0.8.3-spark3.5-s_2.12,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --repositories https://repos.spark-packages.org \
        /tmp/prefetch.py \
    && python -c "import pathlib,shutil; dest=pathlib.Path('/opt/spark/jars'); wanted=('spark-sql-kafka','spark-token-provider-kafka','kafka-clients','commons-pool2','delta-spark','delta-storage','hadoop-aws','aws-java-sdk-bundle','graphframes'); \
roots=[pathlib.Path('/root/.ivy2'), pathlib.Path('/home/spark/.ivy2')]; \
[shutil.copy2(jar, dest/jar.name) for root in roots if root.exists() for jar in root.rglob('*.jar') if any(token in jar.name for token in wanted) and not (dest/jar.name).exists()]; \
print(sorted(p.name for p in dest.glob('graphframes*.jar')))"

WORKDIR /work
