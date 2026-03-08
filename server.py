import grpc
from concurrent import futures
import time

import service_pb2
import service_pb2_grpc

from database import get_connection, init_db


class StudentService(service_pb2_grpc.StudentServiceServicer):

    def __init__(self, port):
        self.port = port
        self.counter = 0 
        
    def replicate_to_peers_add(self, request):

        servers = ["127.0.0.1:50051", "127.0.0.1:50052", "127.0.0.1:50053"]

        for s in servers:

            if s.endswith(str(self.port)):
                continue

            try:
                channel = grpc.insecure_channel(s)
                stub = service_pb2_grpc.StudentServiceStub(channel)

                stub.AddStudent(
                    service_pb2.StudentRequest(
                        id=request.id,
                        name=request.name,
                        age=request.age,
                        replica=True
                    )
                )

            except Exception:
                print("Replication failed to", s)
                
    def replicate_to_peers_update(self, request):

        servers = ["127.0.0.1:50051", "127.0.0.1:50052", "127.0.0.1:50053"]

        for s in servers:

            if s.endswith(str(self.port)):
                continue

            try:
                channel = grpc.insecure_channel(s)
                stub = service_pb2_grpc.StudentServiceStub(channel)

                stub.UpdateStudent(
                    service_pb2.StudentRequest(
                        id=request.id,
                        name=request.name,
                        age=request.age,
                        replica=True
                    )
                )

            except Exception:
                print("Replication failed to", s)
                
    def replicate_to_peers_delete(self, request):

        servers = ["127.0.0.1:50051", "127.0.0.1:50052", "127.0.0.1:50053"]

        for s in servers:

            if s.endswith(str(self.port)):
                continue

            try:
                channel = grpc.insecure_channel(s)
                stub = service_pb2_grpc.StudentServiceStub(channel)

                stub.DeleteStudent(
                    service_pb2.StudentId(
                        id=request.id,
                        replica=True
                    )
                )

            except Exception:
                print("Replication failed to", s)

    def AddStudent(self, request, context):
        
        self.counter += 1
        print(f"[Server {self.port}] handled {self.counter} requests")

        print(f"[Server {self.port}] AddStudent request: id={request.id}, name={request.name}, age={request.age}")

        conn = get_connection(self.port)
        cursor = conn.cursor()

        cursor.execute(
            "INSERT OR REPLACE INTO students VALUES (?, ?, ?)",
            (request.id, request.name, request.age)
        )

        conn.commit()
        conn.close()
            
        if not request.replica:
            self.replicate_to_peers_add(request)

        return service_pb2.StudentResponse(
            id=request.id,
            name=request.name,
            age=request.age
        )


    def GetStudent(self, request, context):
        self.counter += 1
        print(f"[Server {self.port}] handled {self.counter} requests")

        print(f"[Server {self.port}] GetStudent request: id={request.id}")

        conn = get_connection(self.port)
        cursor = conn.cursor()

        cursor.execute("SELECT id,name,age FROM students WHERE id=?", (request.id,))
        row = cursor.fetchone()

        conn.close()

        if row is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Student not found")
            return service_pb2.StudentResponse()

        return service_pb2.StudentResponse(
            id=row[0],
            name=row[1],
            age=row[2]
        )


    def ListStudents(self, request, context):
        self.counter += 1
        print(f"[Server {self.port}] handled {self.counter} requests")

        print(f"[Server {self.port}] ListStudents request")
        
        # demo server 50051 bị quá tải, chậm phản hồi, request sẽ gửi đến server 50052 hoặc 50053 để xử lý thay vì bị timeout
        if int(self.port) == 50051:
            print("Server 50051 overloaded...")
            time.sleep(10)

        conn = get_connection(self.port)
        cursor = conn.cursor()

        cursor.execute("SELECT id,name,age FROM students")
        rows = cursor.fetchall()

        conn.close()

        students = []

        for r in rows:
            students.append(
                service_pb2.StudentResponse(
                    id=r[0],
                    name=r[1],
                    age=r[2]
                )
            )

        return service_pb2.StudentList(students=students)
    
    def UpdateStudent(self, request, context):
        self.counter += 1
        print(f"[Server {self.port}] handled {self.counter} requests")

        print(f"[Server {self.port}] UpdateStudent request: id={request.id}, name={request.name}, age={request.age}")

        conn = get_connection(self.port)
        cursor = conn.cursor()

        cursor.execute(
            "UPDATE students SET name=?, age=? WHERE id=?",
            (request.name, request.age, request.id)
        )

        conn.commit()
        conn.close()
        
        if not request.replica:
            self.replicate_to_peers_update(request)

        return service_pb2.StudentResponse(
            id=request.id,
            name=request.name,
            age=request.age
        )
        
    def DeleteStudent(self, request, context):
        self.counter += 1
        print(f"[Server {self.port}] handled {self.counter} requests")

        print(f"[Server {self.port}] DeleteStudent request: id={request.id}")

        conn = get_connection(self.port)
        cursor = conn.cursor()

        cursor.execute(
            "DELETE FROM students WHERE id=?",
            (request.id,)
        )

        conn.commit()
        conn.close()

        if not request.replica:
            self.replicate_to_peers_delete(request)

        return service_pb2.DeleteResponse(
            success=True,
            message=f"Student with id={request.id} deleted"
        )
    def HealthCheck(self, request, context):

        print(f"[Server {self.port}] HealthCheck")

        return service_pb2.HealthResponse(
            status="SERVING",
            port=self.port
        )


def serve(port): # khởi tạo và chạy server gRPC trên cổng đã chọn

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) # server có 10 thread để xử lý các request đồng thời

    service_pb2_grpc.add_StudentServiceServicer_to_server( # đăng ký service vào server
        StudentService(port), server
    )

    server.add_insecure_port(f"[::]:{port}") # mở port để server lắng nghe các request đến

    server.start() # khởi động server

    print(f"Server running at port {port}")

    server.wait_for_termination() # duy trì server hoạt động cho đến khi có tín hiệu dừng


if __name__ == "__main__":

    import sys

    port = int(sys.argv[1])
    
    init_db(port) # Khởi tạo database

    serve(port)