# Use python image as a base
FROM python:3.11

# Install dockerize tool
RUN apt-get update && apt-get install -y wget \
    && wget https://github.com/jwilder/dockerize/releases/download/v0.6.1/dockerize-linux-amd64-v0.6.1.tar.gz \
    && tar -xzvf dockerize-linux-amd64-v0.6.1.tar.gz -C /usr/local/bin/ \
    && rm dockerize-linux-amd64-v0.6.1.tar.gz

# Set the working directory
WORKDIR /app

# Copy the local script into the container
COPY . /app

# Install required Python libraries
RUN pip install -r requirements.txt

# Default command to run the initialization script
CMD ["dockerize", "-wait", "tcp://postgres:5432", "-timeout", "30s", "python", "init_db.py"]