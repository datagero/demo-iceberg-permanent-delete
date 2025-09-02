# Stage 1: Build the environment with Jupyter
FROM apache/spark:v3.3.0 as builder

USER root

# Install Python, pip, and venv
RUN apt-get update && \
    apt-get install -y python3-pip python3-venv unzip && \
    rm -rf /var/lib/apt/lists/*

# Create a virtual environment and install Jupyter
RUN python3 -m venv /opt/spark/venv && \
    /opt/spark/venv/bin/pip install --upgrade pip && \
    /opt/spark/venv/bin/pip install jupyter

# Stage 2: Final image
FROM apache/spark:v3.3.0

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/spark/venv /opt/spark/venv

# Set the PATH to use the virtual environment's Python and pip
ENV PATH="/opt/spark/venv/bin:$PATH"
