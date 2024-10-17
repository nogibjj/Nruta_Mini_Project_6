#Starting the Dockerfile
# Use the official Python base image
FROM python:3.10-slim

# Set the working directory
WORKDIR /workspace

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# Command to run when the container starts
CMD ["python3", "main.py"]