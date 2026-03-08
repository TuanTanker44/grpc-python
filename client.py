import grpc
import random
import service_pb2
import service_pb2_grpc


def connect_server(port):
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = service_pb2_grpc.StudentServiceStub(channel)
    return channel, stub

def health_check(server):

    try:
        channel = grpc.insecure_channel(server)
        stub = service_pb2_grpc.StudentServiceStub(channel)

        res = stub.HealthCheck(service_pb2.Empty(), timeout=2)

        print(f"{server} -> {res.status}")

        return True

    except:
        print(f"{server} -> DOWN")
        return False

def main():

    channel = grpc.insecure_channel(
        "ipv4:127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053",
        options=[("grpc.lb_policy_name", "round_robin")] # sử dụng load balancing round robin để phân phối request đều giữa các server
    )

    stub = service_pb2_grpc.StudentServiceStub(channel) # tạo stub để gọi các phương thức của service trên server

    # connect
    # INSERT id name age
    # SELECT id
    # SELECT ALL
    # UPDATE id name age
    # DELETE id
    # exit

    while True:

        cmd = input("Student SQL Console> ").strip()

        if cmd == "exit":
            break

        parts = cmd.split()

        try:
            if parts[0].upper() == "CONNECT":

                port = parts[1]
                channel, stub = connect_server(port)

                print("Connected to server at port", port)
            

            elif parts[0].upper() == "INSERT":

                id = int(parts[1])
                name = parts[2]
                age = int(parts[3])

                res = stub.AddStudent(
                    service_pb2.StudentRequest(
                        id=id,
                        name=name,
                        age=age
                    ),
                    timeout=5
                )

                print(f"Inserted: id={res.id}, name={res.name}, age={res.age}")

            elif parts[0].upper() == "SELECT":

                if parts[1].upper() == "ALL":

                    res = stub.ListStudents(service_pb2.Empty(),timeout=5)

                    for s in res.students:
                        print(f"id={s.id}, name={s.name}, age={s.age}")

                else:

                    id = int(parts[1])

                    res = stub.GetStudent(
                        service_pb2.StudentId(id=id),timeout=5
                    )

                    print(f"id={res.id}, name={res.name}, age={res.age}")

            elif parts[0].upper() == "UPDATE":

                id = int(parts[1])
                name = parts[2]
                age = int(parts[3])

                res = stub.UpdateStudent(
                    service_pb2.StudentRequest(
                        id=id,
                        name=name,
                        age=age
                    ),
                    timeout=5
                )

                print(f"Updated: id={res.id}, name={res.name}, age={res.age}")

            elif parts[0].upper() == "DELETE":

                id = int(parts[1])

                res = stub.DeleteStudent(
                    service_pb2.StudentId(id=id),
                    timeout=5
                )

                print(f"Deleted: {res.message}")
                
                
            elif parts[0].upper() == "HEALTH":
                
                if len(parts) < 2:
                    print("Usage: health <port|all>")
                    continue
                
                if parts[1].upper() == "ALL":
                    servers = [
                        "127.0.0.1:50051",
                        "127.0.0.1:50052",
                        "127.0.0.1:50053"
                    ]
                    
                    serving_counter = 0

                    for server in servers:
                        status = health_check(server)

                        if status == True:
                            serving_counter += 1

                    
                    if serving_counter == len(servers):
                        print("all -> SERVING")
                    elif serving_counter > 0:
                        print("all -> DEGRADED")
                        print(f"available: {serving_counter} / {len(servers)}")
                    else:
                        print("all -> DOWN")
                        
                else:
                    health_check(parts[1])

                
            else:
                print("Unknown command")

        except Exception as e:
            print("Error:", e)
            


if __name__ == "__main__":
    main()