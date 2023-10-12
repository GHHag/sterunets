import grpc
from concurrent import futures


def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    server.add_insecure_port('[::]:5001')
    server.start()

    server.wait_for_termination()


if __name__ == '__main__':
    run_grpc_server()
