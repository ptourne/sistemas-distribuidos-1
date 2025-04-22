import grpc 
# pip install grpcio grpcio-tools
# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. sentiment.proto

from concurrent import futures
from transformers import pipeline
import sentiment_pb2 as sentiment_pb2
import sentiment_pb2_grpc as sentiment_pb2_grpc

sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')

class SentimentServicer(sentiment_pb2_grpc.SentimentAnalyzerServicer):
    def Analyze(self, request, context):
        result = sentiment_analyzer(request.text)[0]
        return sentiment_pb2.SentimentResponse(label=result['label'], score=result['score'])

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    sentiment_pb2_grpc.add_SentimentAnalyzerServicer_to_server(SentimentServicer(), server)
    server.add_insecure_port('0.0.0.0:50051')
    server.start()
    print("Sentiment service running on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
