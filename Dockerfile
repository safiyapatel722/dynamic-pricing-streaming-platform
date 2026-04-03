FROM python:3.11-slim

WORKDIR /app

# install dependencies first — Docker layer caching means
# this layer only rebuilds when requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy source code after dependencies
COPY . .

# default port for Cloud Run
EXPOSE 8080