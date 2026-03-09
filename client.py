import grpc
import service_pb2
import service_pb2_grpc


SERVERS = [
    "127.0.0.1:50051",
    "127.0.0.1:50052",
    "127.0.0.1:50053"
]


def create_stub(server):
    channel = grpc.insecure_channel(server)
    stub = service_pb2_grpc.StudentServiceStub(channel)
    return stub


def call_with_failover(method, request, timeout=5):
    last_error = None

    for server in SERVERS:
        try:
            stub = create_stub(server)

            rpc_method = getattr(stub, method)

            response = rpc_method(request, timeout=timeout)

            print(f"[SERVER] handled by {server}")

            return response

        except Exception as e:
            print(f"[FAIL] {server} -> {e}")
            last_error = e

    raise last_error


def health_check(server):
    try:
        stub = create_stub(server)

        res = stub.HealthCheck(service_pb2.Empty(), timeout=2)

        print(f"{server} -> {res.status}")

        return True

    except:
        print(f"{server} -> DOWN")
        return False


def main():

    print("Student SQL Console")
    print("Commands:")
    print("INSERT id name age")
    print("SELECT id")
    print("SELECT ALL")
    print("UPDATE id name age")
    print("DELETE id")
    print("HEALTH port|ALL")
    print("exit")

    while True:

        cmd = input("Student SQL Console> ").strip()

        if cmd == "exit":
            break

        parts = cmd.split()

        try:

            if parts[0].upper() == "INSERT":

                id = int(parts[1])
                name = parts[2]
                age = int(parts[3])

                res = call_with_failover(
                    "AddStudent",
                    service_pb2.StudentRequest(
                        id=id,
                        name=name,
                        age=age
                    )
                )

                print(f"Inserted: id={res.id}, name={res.name}, age={res.age}")


            elif parts[0].upper() == "SELECT":

                if parts[1].upper() == "ALL":

                    res = call_with_failover(
                        "ListStudents",
                        service_pb2.Empty()
                    )

                    for s in res.students:
                        print(f"id={s.id}, name={s.name}, age={s.age}")

                else:

                    id = int(parts[1])

                    res = call_with_failover(
                        "GetStudent",
                        service_pb2.StudentId(id=id)
                    )

                    print(f"id={res.id}, name={res.name}, age={res.age}")


            elif parts[0].upper() == "UPDATE":

                id = int(parts[1])
                name = parts[2]
                age = int(parts[3])

                res = call_with_failover(
                    "UpdateStudent",
                    service_pb2.StudentRequest(
                        id=id,
                        name=name,
                        age=age
                    )
                )

                print(f"Updated: id={res.id}, name={res.name}, age={res.age}")


            elif parts[0].upper() == "DELETE":

                id = int(parts[1])

                res = call_with_failover(
                    "DeleteStudent",
                    service_pb2.StudentId(id=id)
                )

                print(f"Deleted: {res.message}")


            elif parts[0].upper() == "HEALTH":

                if len(parts) < 2:
                    print("Usage: HEALTH <port|ALL>")
                    continue

                if parts[1].upper() == "ALL":

                    serving_counter = 0

                    for server in SERVERS:

                        status = health_check(server)

                        if status:
                            serving_counter += 1

                    if serving_counter == len(SERVERS):
                        print("all -> SERVING")

                    elif serving_counter > 0:
                        print("all -> DEGRADED")
                        print(f"available: {serving_counter} / {len(SERVERS)}")

                    else:
                        print("all -> DOWN")

                else:

                    port = parts[1]
                    health_check(f"127.0.0.1:{port}")


            else:
                print("Unknown command")

        except Exception as e:
            print("Error:", e)


if __name__ == "__main__":
    main()