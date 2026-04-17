# I customized my own Airflow image using a Dockerfile
# This file will modify the Airflow image according to my requirements.txt file:

# Use the official Airflow image as the base and Adding Git to the image
FROM apache/airflow:2.7.1
USER root
RUN apt-get update && apt-get install -y git && apt-get clean
USER airflow

# Copy the requirements.txt from the local machine into the container
COPY requirements.txt /requirements.txt

# Install the libraries
RUN pip install --no-cache-dir -r /requirements.txt