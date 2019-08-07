import os

from contextlib import contextmanager

try:
    import apsw
except ModuleNotFoundError as e:
    from .Tools import install_apsw

    install_apsw()
    import apsw


class relational_db(object):
    def __init__(self, basedir):
        self.filename = os.path.expanduser(os.path.join(basedir, "db.sqlite"))
        self.db = apsw.Connection(self.filename)

    def cursor(self):
        return self.db.cursor()

    @contextmanager
    def bulk_transaction(self):
        """
            This is a context manager that handles bulk transaction.
            i.e. this context will handle the BEGIN, END and appropriate
            ROLLBACKS.

            Usage:
            >>> with x._bulk_transaction() as cur:
                     cur.execute('INSERT INTO table XXX VALUES YYY')
        """
        cur = self.db.cursor()
        cur.execute("PRAGMA synchronous = off")
        cur.execute("PRAGMA journal_mode = memory")
        cur.execute("SAVEPOINT m80_bulk_transaction")
        try:
            yield cur
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT m80_bulk_transaction")
            raise e
        finally:
            cur.execute("RELEASE SAVEPOINT m80_bulk_transaction")

    def query(self, q):
        cur = self.db.cursor().execute(q)
        names = [x[0] for x in cur.description]
        rows = cur.fetchall()
        result = pd.DataFrame(rows, columns=names)
        return result
