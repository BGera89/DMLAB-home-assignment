# Use a Python base image
FROM python:3.11

# Set the working directory inside the container
WORKDIR /app

# Copy the API code and requirements into the container
COPY . /app

# Install required Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port your FastAPI app runs on
EXPOSE 5000

# Command to run the FastAPI app
CMD ["sh", "-c", "python init_db.py && uvicorn api_fetcher_api:app --host 0.0.0.0 --port 5000"]