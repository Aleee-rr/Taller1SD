FROM python:3.12-slim
WORKDIR /srv
COPY requirements-t2.txt .
RUN pip install --no-cache-dir -r requirements-t2.txt
COPY common ./common
COPY coordinador ./coordinador
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "coordinador.api:app", "--host", "0.0.0.0", "--port", "8000"]
