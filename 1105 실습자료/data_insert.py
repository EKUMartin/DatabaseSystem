# %%
import pymysql
import pandas as pd

db_config = {
    "host": "localhost",
    "user": "DB계정",
    "password": "비밀번호"
    "database": "스키마이름",
    "charset": "utf8mb4",
    "port": 포트번호,
    "cursorclass": pymysql.cursors.DictCursor,
}

connection = pymysql.connect(**db_config)

def create_tables():
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS DEPARTMENT (
                dept_id INT PRIMARY KEY,
                dept_name VARCHAR(100),
                office_location VARCHAR(100),
                tel VARCHAR(20)
            );
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS PROFESSOR (
                prof_id INT PRIMARY KEY,
                prof_name VARCHAR(100),
                dept_id INT,
                email VARCHAR(100),
                position VARCHAR(50),
                hire_date DATE,
                FOREIGN KEY (dept_id) REFERENCES DEPARTMENT(dept_id)
            );
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS COURSE (
                course_id INT PRIMARY KEY,
                course_name VARCHAR(100),
                credits INT,
                prof_id INT,
                max_students INT,
                course_room VARCHAR(50),
                FOREIGN KEY (prof_id) REFERENCES PROFESSOR(prof_id)
            );
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ENROLLMENT (
                enrollment_id INT PRIMARY KEY,
                student_id INT,
                course_id INT,
                semester VARCHAR(10),
                grade VARCHAR(2),
                enroll_date DATE,
                FOREIGN KEY (course_id) REFERENCES COURSE(course_id)
            );
        """
        )
    connection.commit()


def insert_data_from_csv(table_name, csv_file):
    data = pd.read_csv(csv_file)
    columns = ", ".join(data.columns)
    placeholders = ", ".join(["%s"] * len(data.columns))

    with connection.cursor() as cursor:
        for row in data.itertuples(index=False, name=None):
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            try:
                cursor.execute(sql, row)
            except pymysql.MySQLError as e:
                print(f"Error inserting into {table_name}: {e}")
                connection.rollback()
                return
        connection.commit()


create_tables()
insert_data_from_csv("DEPARTMENT", "department.csv")
insert_data_from_csv("PROFESSOR", "professor.csv")
insert_data_from_csv("COURSE", "course.csv")
insert_data_from_csv("ENROLLMENT", "enrollment.csv")

connection.close()

print("Data insertion completed.")
# %%
