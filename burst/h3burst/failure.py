"""Generation-bound test failure signal shared by executor and stop supervisor."""

SCHEMA = ('CREATE TABLE IF NOT EXISTS cloud_test_failures '
          '(generation TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, category TEXT NOT NULL)')


def read_failure(journal, generation):
    if not journal.conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                "AND name='cloud_test_failures'").fetchone():
        return None
    row = journal.conn.execute('SELECT attempt_id,category FROM cloud_test_failures WHERE generation=?',
                               (generation,)).fetchone()
    return {'attempt_id': row[0], 'category': row[1]} if row else None
