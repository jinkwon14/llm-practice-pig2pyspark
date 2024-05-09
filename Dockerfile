FROM ollama/ollama

# Install Java and utilities - requirement for Pig
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get install -y default-jdk

# Install Apache Pig
RUN wget http://apache.mirrors.pair.com/pig/pig-0.17.0/pig-0.17.0.tar.gz && \
    tar -xzf pig-0.17.0.tar.gz -C /opt/ && \
    rm pig-0.17.0.tar.gz

# Copy the startup script
COPY setup-env.sh /usr/local/bin/setup-env.sh
RUN chmod +x /usr/local/bin/setup-env.sh

WORKDIR /workspace

COPY requirements.txt requirements.txt


# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip

# 
RUN pip install --upgrade pip && \
     pip install -r requirements.txt

EXPOSE 8888 

ENV HF_TOKEN="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"

ENTRYPOINT ["/usr/local/bin/setup-env.sh", "jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser"]
