import weaviate

def connect_to_local(host: str, port: int, grpc_port: int, headers: dict):
    # Weaviate v4 client
    return weaviate.connect_to_custom(
        http_host=host,
        http_port=port,
        grpc_host=host,
        grpc_port=grpc_port,
        headers=headers,
        http_secure=False,
        grpc_secure=False,
    )