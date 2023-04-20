FROM python:3.10.11

# Set the working directory
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY app/ app/
COPY config/ config/
COPY tests/ tests/

COPY python_commands.sh .
RUN chmod +x python_commands.sh

# Set environment variable for Python module search path
ENV PYTHONPATH="${PYTHONPATH}:/app:/config:tests/"

# Run the application
CMD ["/bin/sh", "python_commands.sh"]