# Use the official Ubuntu base image
FROM ubuntu:latest

# Update and install necessary packages
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Install Pandas using pip
RUN pip3 install pandas

# Copy your application code into the container
COPY . /app

# Command to run your application
CMD [ "python3", "your_script.py" ]
