FROM python:2-onbuild
RUN pip install arrow
ENTRYPOINT ["python", "-m", "pysciencedock"]
