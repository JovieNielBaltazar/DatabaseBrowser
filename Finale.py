import csv
import mysql.connector
import os
from datetime import datetime
import pandas as pd


class DatabaseExporter:
    def __init__(self, host, user, password, database, port):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.cursor = None
        self.file_path = 'C:\\Users\\Joviel Niel Baltazar\\PycharmProjects\\DL-from-DB\\CSV\\'
        self.current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        tables = self.cursor.fetchall()
        table_names = [table[0] for table in tables]
        return table_names

    def show_columns(self, table_name):
        self.cursor.execute(f"DESCRIBE {table_name}")
        columns = self.cursor.fetchall()
        column_names = [column[0] for column in columns]
        return column_names

    def export_table_as_csv(self, table_name, column_names, rows):
        saved_csvfile_path = os.path.join(self.file_path, f"{table_name}_{self.current_datetime}.csv")
        with open(saved_csvfile_path, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(column_names)
            csv_writer.writerows(rows)
        print(f"The table '{table_name}' has been exported to '{saved_csvfile_path}'.")

    def choose_table(self):
        self.connect()

        while True:
            table_names = self.show_tables()

            # Display the list of tables
            print("Available Tables:")
            for i, table_name in enumerate(table_names, 1):
                print(f"{i}. {table_name}")

            # Prompt the user to choose a table
            selected_table_option = input("Choose a table (by number): ")
            if not selected_table_option.isdigit() or int(selected_table_option) <= 0 or int(
                    selected_table_option) > len(table_names):
                print("Invalid input. Please choose a valid table number.")
                continue

            selected_table_option = int(selected_table_option)
            selected_table = table_names[selected_table_option - 1]

            # Prompt the user to choose between viewing the sample table or columns only
            print("\nOptions:")
            print("1. View sample table (first 10 rows)")
            print("2. View columns only")
            print("0. Return or Back")
            display_option = input("Choose an option (by number): ")

            if display_option == "1":
                self.cursor.execute(f"SELECT * FROM {selected_table} LIMIT 10")
                rows = self.cursor.fetchall()
                column_names = [desc[0] for desc in self.cursor.description]

                # Create a dataframe with the column names and rows
                df = pd.DataFrame(rows, columns=column_names)

                # Display the dataframe
                print(f"\nSample table (first 10 rows) of the table '{selected_table}':")
                print(df)

                # Prompt the user for navigation
                return_or_back = input("Press 'Enter' to proceed or input 'back' to choose another table: ")
                if return_or_back.lower() == "back":
                    self.disconnect()
                    return

            elif display_option == "2":
                column_names = self.show_columns(selected_table)

                # Display the column names
                print(f"\nColumns of the table '{selected_table}':")
                for column_name in column_names:
                    print(column_name)

                # Prompt the user for navigation
                return_or_back = input("Press 'Enter' to proceed or input 'back' to choose another table: ")
                if return_or_back.lower() == "back":
                    self.disconnect()
                    return

            elif display_option == "0":
                self.disconnect()
                return  # Return to the table selection prompt

            else:
                print("Invalid input. Please choose a valid option.")
                continue

            # Prompt the user to download the table as a CSV file
            download_csv = input("Download the table as a CSV file? (yes/no): ")

            if download_csv.lower() == "yes":
                self.cursor.execute(f"SELECT * FROM {selected_table}")
                rows = self.cursor.fetchall()
                self.export_table_as_csv(selected_table, column_names, rows)
            else:
                print("Download cancelled.")

            # Prompt the user to choose another table or exit
            if return_or_back.lower() != "back":
                choose_another_table = input("Choose another table? (yes/no): ")
                if choose_another_table.lower() != "yes":
                    self.disconnect()
                    exit()

    def run(self):
        while True:
            self.choose_table()


# Usage
exporter = DatabaseExporter(host='localhost', user='root', password='', database='db1', port=3306)
exporter.run()
