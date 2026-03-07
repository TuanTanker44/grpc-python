import grpc
from concurrent import futures

import service_pb2
import service_pb2_grpc

from database import get_connection, init_db


class StudentService(service_pb2_grpc.StudentServiceServicer):

    def AddStudent(self, request, context):

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "INSERT OR REPLACE INTO students VALUES (?, ?, ?)",
            (request.id, request.name, request.age)
        )

        conn.commit()
        conn.close()

        return service_pb2.StudentResponse(
            id=request.id,
            name=request.name,
            age=request.age
        )

    def GetStudent(self, request, context):

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id,name,age FROM students WHERE id=?",
            (request.id,)
        )

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

        conn = get_connection()
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


def serve(port):

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    service_pb2_grpc.add_StudentServiceServicer_to_server(
        StudentService(), server
    )

    server.add_insecure_port(f"[::]:{port}")

    server.start()

    print(f"Server running at port {port}")

    server.wait_for_termination()


if __name__ == "__main__":

    import sys

    init_db()

    port = sys.argv[1]

    serve(port)