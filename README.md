# gRPC Student Management

## Install dependencies

pip install -r requirements.txt

## Generate proto

python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. service.proto

## Run servers

python server.py 50051
python server.py 50052
python server.py 50053

## Run client

python client.py
