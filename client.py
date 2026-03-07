import grpc
import service_pb2
import service_pb2_grpc


def run():

    channel = grpc.insecure_channel("localhost:50051")

    stub = service_pb2_grpc.StudentServiceStub(channel)

    # Add
    print("\n=== ADD STUDENT ===")

    stub.AddStudent(
        service_pb2.StudentRequest(
            id=1,
            name="Nguyen Van A",
            age=20
        )
    )

    stub.AddStudent(
        service_pb2.StudentRequest(
            id=2,
            name="Tran Thi B",
            age=21
        )
    )

    # List
    print("\n=== LIST STUDENTS ===")

    res = stub.ListStudents(service_pb2.Empty())

    for s in res.students:
        print(s)

    # Update
    print("\n=== UPDATE STUDENT ===")

    res = stub.UpdateStudent(
        service_pb2.StudentRequest(
            id=1,
            name="Nguyen Van A Updated",
            age=22
        )
    )

    print(res)

    # Get
    print("\n=== GET STUDENT ===")

    res = stub.GetStudent(
        service_pb2.StudentId(id=1)
    )

    print(res)

    # Delete
    print("\n=== DELETE STUDENT ===")

    res = stub.DeleteStudent(
        service_pb2.StudentId(id=2)
    )

    print(res.message)

    # List again
    print("\n=== LIST STUDENTS AGAIN ===")

    res = stub.ListStudents(service_pb2.Empty())

    for s in res.students:
        print(s)


if __name__ == "__main__":
    run()