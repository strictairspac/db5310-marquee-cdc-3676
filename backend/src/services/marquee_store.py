"""Connection-scoped InnoDB workspace for one replay."""

from contextlib import contextmanager

from sqlalchemy import bindparam, text


_TABLES = {
    'marquee_cdc_offsets': '''
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
        seq BIGINT UNSIGNED NOT NULL
    ''',
    'marquee_cdc_projection': '''
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        sign_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        content VARCHAR(240) NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (stream, sign_id)
    ''',
    'marquee_cdc_journal': '''
        receipt INT UNSIGNED PRIMARY KEY,
        batch_no BIGINT UNSIGNED NOT NULL,
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        event_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        sign_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        operation ENUM('put', 'delete') NOT NULL,
        content VARCHAR(240) NOT NULL,
        state ENUM('pending', 'applied', 'conflict', 'history', 'adjudicated') NOT NULL,
        reason VARCHAR(240) NOT NULL,
        ruling_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
        KEY stream_sequence (stream, seq),
        KEY stream_event (stream, event_id)
    ''',
    'marquee_cdc_adjudications': '''
        ruling_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
        batch_no BIGINT UNSIGNED NOT NULL,
        stream VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        event_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
        selected_receipts VARCHAR(2000) NOT NULL,
        sign_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin,
        operation ENUM('put', 'delete'),
        content VARCHAR(240),
        state ENUM('held', 'effective', 'released', 'lapsed', 'rejected') NOT NULL,
        reason VARCHAR(240) NOT NULL,
        KEY stream_sequence (stream, seq)
    ''',
}


@contextmanager
def isolated_replay(engine):
    if engine.dialect.name != 'mysql':
        raise ValueError('牌面回放需要 MySQL 8.0')
    with engine.connect().execution_options(isolation_level='READ COMMITTED') as conn:
        try:
            with conn.begin():
                for table, columns in _TABLES.items():
                    conn.exec_driver_sql(
                        f'CREATE TEMPORARY TABLE {table} ({columns}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
                    )
            yield conn
        finally:
            if not conn.invalidated:
                try:
                    conn.rollback()
                    with conn.begin():
                        conn.exec_driver_sql(f'DROP TEMPORARY TABLE IF EXISTS {", ".join(_TABLES)}')
                except Exception:
                    conn.invalidate()
                    raise


def initialize(conn, checkpoints, snapshot):
    with conn.begin():
        conn.execute(text('INSERT INTO marquee_cdc_offsets (stream, seq) VALUES (:stream, :sequence)'), checkpoints)
        if snapshot:
            conn.execute(text('''
                INSERT INTO marquee_cdc_projection (stream, sign_id, content, seq)
                VALUES (:stream, :sign_id, :content, :sequence)
            '''), snapshot)


def journal_matches(conn, event):
    return conn.execute(text('''
        SELECT receipt, seq, event_id, sign_id, operation, content, state, ruling_id
        FROM marquee_cdc_journal
        WHERE stream = :stream AND (seq = :seq OR event_id = :event_id)
        ORDER BY receipt
    '''), event).mappings().all()


def insert_receipt(conn, event, state, reason, ruling_id=None):
    conn.execute(text('''
        INSERT INTO marquee_cdc_journal
            (receipt, batch_no, stream, seq, event_id, sign_id, operation, content, state, reason, ruling_id)
        VALUES (:receipt, :batch_no, :stream, :seq, :event_id, :sign_id, :operation, :content, :state, :reason, :ruling_id)
    '''), {**event, 'state': state, 'reason': reason, 'ruling_id': ruling_id})


def quarantine_pending(conn, event):
    conn.execute(text('''
        UPDATE marquee_cdc_journal SET state = 'conflict', reason = :reason
        WHERE stream = :stream AND state = 'pending' AND (seq = :seq OR event_id = :event_id)
    '''), {**event, 'reason': f'与接收行 {event["receipt"]} 的事件序号或事件编号重叠，信封内容不同'})


def waiting_events(conn, stream, offset):
    return conn.execute(text('''
        SELECT receipt, stream, seq, sign_id, operation, content, state, ruling_id
        FROM marquee_cdc_journal
        WHERE stream = :stream AND seq > :offset AND state IN ('pending', 'conflict', 'adjudicated')
        ORDER BY seq, receipt
    '''), {'stream': stream, 'offset': offset}).mappings().all()


def apply_event(conn, event):
    if event['operation'] == 'delete':
        conn.execute(text('''
            DELETE FROM marquee_cdc_projection WHERE stream = :stream AND sign_id = :sign_id
        '''), event)
    else:
        conn.execute(text('''
            INSERT INTO marquee_cdc_projection (stream, sign_id, content, seq)
            VALUES (:stream, :sign_id, :content, :seq)
            ON DUPLICATE KEY UPDATE content = :content, seq = :seq
        '''), event)
    conn.execute(text('''
        UPDATE marquee_cdc_journal SET state = 'applied' WHERE receipt = :receipt
    '''), event)


def read_offsets(conn):
    return dict(conn.execute(text('SELECT stream, seq FROM marquee_cdc_offsets ORDER BY stream')).all())


def advance_offset(conn, stream, seq):
    conn.execute(text('UPDATE marquee_cdc_offsets SET seq = :seq WHERE stream = :stream'),
                 {'stream': stream, 'seq': seq})


def read_projection(conn):
    return conn.execute(text('''
        SELECT stream, sign_id, content, seq FROM marquee_cdc_projection ORDER BY stream, sign_id
    ''')).mappings().all()


def read_journal(conn):
    return conn.execute(text('SELECT * FROM marquee_cdc_journal ORDER BY receipt')).mappings().all()


def insert_ruling(conn, ruling, state, reason):
    conn.execute(text('''
        INSERT INTO marquee_cdc_adjudications
            (ruling_id, batch_no, stream, seq, event_id, selected_receipts,
             sign_id, operation, content, state, reason)
        VALUES (:ruling_id, :batch_no, :stream, :seq, :event_id, :selected_receipts,
                :sign_id, :operation, :content, :state, :reason)
    '''), {**ruling, 'selected_receipts': ','.join(str(item) for item in ruling['selected_receipts']),
           'state': state, 'reason': reason})


def read_rulings(conn):
    return conn.execute(text('SELECT * FROM marquee_cdc_adjudications ORDER BY batch_no, ruling_id')).mappings().all()


def get_ruling(conn, ruling_id):
    return conn.execute(text('SELECT * FROM marquee_cdc_adjudications WHERE ruling_id = :ruling_id'),
                        {'ruling_id': ruling_id}).mappings().first()


def seq_rulings(conn, stream, seq):
    """All still-open rulings (held gap rulings or effective group rulings) for one sequence."""
    return conn.execute(text('''
        SELECT * FROM marquee_cdc_adjudications
        WHERE stream = :stream AND seq = :seq AND state IN ('held', 'effective')
        ORDER BY batch_no, ruling_id
    '''), {'stream': stream, 'seq': seq}).mappings().all()


def lapse_held(conn, stream, seq, reason):
    conn.execute(text('''
        UPDATE marquee_cdc_adjudications SET state = 'lapsed', reason = :reason
        WHERE stream = :stream AND seq = :seq AND state = 'held'
    '''), {'stream': stream, 'seq': seq, 'reason': reason})


def update_ruling(conn, ruling_id, state, reason):
    conn.execute(text('''
        UPDATE marquee_cdc_adjudications SET state = :state, reason = :reason
        WHERE ruling_id = :ruling_id
    '''), {'ruling_id': ruling_id, 'state': state, 'reason': reason})


def mark_adjudicated(conn, stream, receipts, ruling_id, reason):
    conn.execute(text('''
        UPDATE marquee_cdc_journal
        SET state = 'adjudicated', ruling_id = :ruling_id, reason = :reason
        WHERE stream = :stream AND receipt IN :receipts
    ''').bindparams(bindparam('receipts', expanding=True)),
        {'stream': stream, 'ruling_id': ruling_id, 'reason': reason, 'receipts': list(receipts)})


def ruling_remaining(conn, receipts):
    """Selected receipts not yet applied: a ruling is released only when all are applied."""
    return conn.execute(text('''
        SELECT COUNT(*) FROM marquee_cdc_journal
        WHERE receipt IN :receipts AND state <> 'applied'
    ''').bindparams(bindparam('receipts', expanding=True)),
        {'receipts': list(receipts)}).scalar_one()
