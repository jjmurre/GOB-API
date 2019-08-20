FROM amsterdam/docker_python:latest
MAINTAINER datapunt@amsterdam.nl

# Install ANTLR
RUN apt-get update -y
RUN apt-get install -y antlr4

# Install api service in /app folder
WORKDIR /app

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gob api module
COPY gobapi gobapi

# Build
COPY build.sh /app
RUN ./build.sh

CMD uwsgi
