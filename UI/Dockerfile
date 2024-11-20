# Use a Python base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the Dash app files (including your Python script and requirements) into the container
COPY . /app

# Install required Python libraries from the requirements file
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 8050 for the Dash app
EXPOSE 8050

# Command to run the Dash app
CMD ["python", "app.py"]