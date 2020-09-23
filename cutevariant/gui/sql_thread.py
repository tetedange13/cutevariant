from PySide2.QtCore import QThread
import sqlite3


class SqlThread(QThread):

    """Call a sql query connection from a Thread. 
    Get results when thread is finished.
    
    Usage:

    thread = SqlThread(conn)
    thread.execute("SELECT ..")
    thread.finished.connect(lambda : print(thread.results))

    """

    def __init__(self, conn: sqlite3.Connection, parent=None):
        super().__init__(parent)
        # Get filename and create a new conn

        if conn:
            self.filename = conn.execute("PRAGMA database_list").fetchone()["file"]
        else:
            self.filename = None

        self.query = None
        self.function = None
        self.results = None

    def exec_query(self, query):
        if self.filename:
            self.query = query
            self.start()

    def exec_function(self, function):
        if self.filename:
            self.function = function
            self.start()

    def run(self):
        # We are in a new thread ...
        self.async_conn = sqlite3.Connection(self.filename)
        self.async_conn.row_factory = sqlite3.Row

        if self.function:
            self.results = self.function(self.async_conn)
            return

        if self.query:
            self.results = list(self.async_conn.execute(self.query).fetchall())
            return

        self.query = None
        self.function = None
