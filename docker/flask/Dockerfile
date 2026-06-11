FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY resources/web/ .

ENV PROJECT_HOME=/app
ENV FLASK_APP=predict_flask.py

EXPOSE 5001

CMD ["python3", "predict_flask.py"]
