FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir transformers torch grpcio grpcio-tools hf_xet huggingface_hub[hf_xet] && rm -rf /root/.cache

RUN python -c "from transformers import pipeline; pipeline('sentiment-analysis')"

COPY worker/nlp/python_server/ .

CMD ["python", "nlp.py"]