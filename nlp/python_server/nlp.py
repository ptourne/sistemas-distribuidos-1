import grpc 
# pip install grpcio grpcio-tools
# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. sentiment.proto
import os
import signal
import sys
from concurrent import futures
from transformers import pipeline
import sentiment_pb2 as sentiment_pb2
import sentiment_pb2_grpc as sentiment_pb2_grpc
from heartbeat import start_heartbeat

sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')

class SentimentServicer(sentiment_pb2_grpc.SentimentAnalyzerServicer):
    def Analyze(self, request, context):
        result = sentiment_analyzer(request.text, truncation=True)[0]
        return sentiment_pb2.SentimentResponse(label=result['label'], score=result['score'])

def serve(workers, port, heartbeat_proc, stop_event):
 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    sentiment_pb2_grpc.add_SentimentAnalyzerServicer_to_server(SentimentServicer(), server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()

    def handle_sigterm(*args):
        print("Received SIGTERM. Shuting down gracefully...")
        stop_event.set()
        heartbeat_proc.join()
        server.stop(grace=5) 

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    server.wait_for_termination()

if __name__ == '__main__':
    port = os.getenv("GRPC_PORT", "50051")
    workers = int(os.getenv("GRPC_WORKERS", "5"))
    name = os.getenv("NAME", "sentiment_server")
    monitor_addrs = os.getenv("MONITOR_ADDRESSES", "")
    stop_event, heartbeat_proc = start_heartbeat(name, monitor_addrs)
    serve(workers, port, heartbeat_proc, stop_event)
