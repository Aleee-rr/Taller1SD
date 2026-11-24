FROM python:3.12-slim
WORKDIR /srv
COPY requirements-t2.txt .
RUN pip install --no-cache-dir -r requirements-t2.txt
COPY common ./common
COPY agente ./agente
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "agente.agente", "demonio"]
