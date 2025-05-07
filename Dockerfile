FROM ubuntu:20.04

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Update system and install Metastudent
RUN apt-get update && apt-get install -y \
    metastudent \
    && apt-get clean

# Set working directory
WORKDIR /app

# Mount input/output paths dynamically and run metastudent
ENTRYPOINT ["metastudent"]
