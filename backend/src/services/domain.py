"""Replay ordered change capture for the market's stall signage.

Upstream adjudications (裁定) resolve conflicts and missing sequences:

* a ruling binds every record implicated by the same sequence or event_id as
  one group (transitive closure);
* selected receipts are pinned and cannot clash again;
* a missing-sequence ruling is held until the real record arrives — a matching
  envelope makes it effective, a different version makes it lapse;
* identical redelivery and concurrent arrivals follow the same staging rules;
* group release lands together with the projection and the offset in one
  transaction, so a failed retry never releases twice;
* later rulings never touch already submitted changes.
"""

import re
from itertools import groupby
from operator import itemgetter

from sqlalchemy import bindparam, text

from src.services import marquee_store as store


_MAX_SEQUENCE = 2**53 - 1
_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}')
_ENVELOPE = ('seq', 'event_id', 'sign_id', 'operation', 'content')
_OPERATIONS = {'put': '更新牌面', 'delete': '撤下牌面'}
_STATES = {'pending': '暂缓', 'applied': '已提交', 'conflict': '矛盾隔离',
           'history': '历史未核实', 'adjudicated': '裁定放行'}
_RULING_STATES = {'held': '挂起待号', 'effective': '已生效待放行', 'released': '整组已放行',
                  'lapsed': '原裁定失效', 'rejected': '裁定驳回'}
_RULING_FIELDS = ('sign_id', 'operation', 'content')


def _sample_event(batch, stream, sequence, sign_id, content, operation='put', event_id=None):
    return dict(batch=batch, stream=stream, sequence=sequence,
                event_id=event_id or f'E-{sequence}', sign_id=sign_id,
                operation=operation, content=content)


SPEC = {
    'name': '灯窗市集 08:00 开市牌面发布',
    'summary': '按档口流的连续序号发布牌面更新，保留缺位与矛盾的证据，支持上游补交裁定整组放行。',
    'customer': '灯窗市集·营运发布组',
    'entity_labels': ['牌面回放档案', '断点续放', '牌面发布单'],
    'field_labels': {
        'checkpoints': '起始消费位点', 'snapshot': '位点对应的在用牌面快照', 'events': '到货事件',
        'adjudications': '上游补交裁定', 'stream': '分区流', 'sequence': '流内序号',
        'batch': '到货批号', 'event_id': '事件编号', 'sign_id': '牌面编号',
        'operation': '动作（put / delete）', 'content': '牌面内容',
        'ruling_id': '裁定编号', 'selected_receipts': '选中的接收行',
    },
    'guidance': ('录入昨夜位点与完整牌面快照，再按交付次序填写到货批号。同批允许乱序，撤牌动作填 delete，内容留空。'
                 '矛盾或缺号可由上游补交裁定：选中接收行即整组裁定，缺号也可先给裁定信封；'
                 '缺号等到不同版本时原裁定自动失效，补裁定不会改动已提交的牌面。'),
    'sample': {
        'checkpoints': [
            {'stream': 'NORTH', 'sequence': 12},
            {'stream': 'SOUTH', 'sequence': 4},
            {'stream': 'WEST', 'sequence': 0},
        ],
        'snapshot': [
            {'stream': 'NORTH', 'sign_id': 'N-07', 'sequence': 12, 'content': '桂花米酿｜22元'},
            {'stream': 'NORTH', 'sign_id': 'N-08', 'sequence': 11, 'content': '青团｜12元'},
            {'stream': 'SOUTH', 'sign_id': 'S-02', 'sequence': 4, 'content': '烤梨｜16元'},
        ],
        'events': [
            _sample_event(1, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
            _sample_event(1, 'SOUTH', 5, 'S-02', '烤梨｜14元'),
            _sample_event(1, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
            _sample_event(1, 'WEST', 1, 'W-03', '冰粉｜10元'),
            _sample_event(2, 'NORTH', 16, 'N-10', '杏仁茶｜12元'),
            _sample_event(2, 'NORTH', 15, 'N-09', '红糖糍粑｜15元'),
            _sample_event(2, 'NORTH', 13, 'N-08', '', 'delete'),
            _sample_event(2, 'NORTH', 15, 'N-09', '红糖糍粑｜17元'),
            _sample_event(2, 'SOUTH', 7, 'S-02', '烤梨｜13元'),
            _sample_event(2, 'WEST', 2, 'W-03', '', 'delete'),
            _sample_event(3, 'SOUTH', 5, 'S-02', '烤梨｜15元'),
            _sample_event(3, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
            # 07:54 复核员把南区缺号 6 交上游，北区 15 的两种报价进入核对。
            # 批号 5：北区 14 原样重投再次忽略；南区缺号 6 来了与裁定不同的版本，原裁定失效。
            _sample_event(5, 'NORTH', 14, 'N-07', '桂花米酿｜18元'),
            _sample_event(5, 'SOUTH', 6, 'S-02', '烤梨｜13元'),
            # 批号 6：上游重提南区 6 裁定并选中真实接收行；西区 3 两种版本由新裁定整组放行。
            _sample_event(6, 'WEST', 3, 'W-04', '糖炒栗子｜12元', event_id='E-3A'),
            _sample_event(6, 'WEST', 3, 'W-04', '糖炒栗子｜10元', event_id='E-3B'),
        ],
        'adjudications': [
            # 北区 15 两个矛盾信封，整组牵连后选中接收行 6（15 元）。
            {'batch': 4, 'ruling_id': 'A-15-1', 'stream': 'NORTH', 'sequence': 15,
             'event_id': 'E-15', 'selected_receipts': [6]},
            # 南区缺号 6：号还没到，先挂起裁定信封（14 元）。
            {'batch': 4, 'ruling_id': 'A-06-1', 'stream': 'SOUTH', 'sequence': 6,
             'selected_receipts': [], 'sign_id': 'S-02', 'operation': 'put', 'content': '烤梨｜14元'},
            # 批号 6：缺号已到且与新裁定一致，选中真实接收行 14，整组放行。
            {'batch': 6, 'ruling_id': 'A-06-2', 'stream': 'SOUTH', 'sequence': 6,
             'event_id': 'E-6', 'selected_receipts': [14]},
            # 西区 2 已提交过撤牌，补裁定不可改写已提交变更，应被驳回。
            {'batch': 6, 'ruling_id': 'A-02-1', 'stream': 'WEST', 'sequence': 2,
             'event_id': 'E-2', 'selected_receipts': [10]},
            # 西区 3 晚到矛盾：两种新版本按整组裁定，选中接收行 15。
            {'batch': 6, 'ruling_id': 'A-03-2', 'stream': 'WEST', 'sequence': 3,
             'event_id': 'E-3A', 'selected_receipts': [15]},
        ],
    },
    'expect': {'已提交事件数': 10, '重投忽略数': 3, '矛盾隔离事件数': 1, '历史未核实数': 0,
               '暂缓事件数': 0, '缺口流数': 0, '矛盾阻塞流数': 0, '在用牌面数': 5,
               '裁定挂起数': 0, '裁定生效数': 0, '裁定放行数': 3, '裁定失效数': 1, '裁定驳回数': 1},
}


def _rows(payload, key, limit):
    value = payload.get(key)
    if not isinstance(value, list) or len(value) > limit or any(not isinstance(row, dict) for row in value):
        raise ValueError(f'「{SPEC["field_labels"][key]}」需为最多 {limit} 行的对象数组')
    return value


def _identifier(row, field, label):
    value = row.get(field)
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f'{label}的「{SPEC["field_labels"][field]}」需为 1–64 位字母、数字、点、下划线或连字符，首位须为字母或数字')
    return value


def _optional_identifier(row, field, label):
    value = row.get(field)
    if value is None:
        return None
    return _identifier(row, field, label)


def _number(row, field, minimum, label):
    value = row.get(field)
    if type(value) is not int or not minimum <= value <= _MAX_SEQUENCE:
        raise ValueError(f'{label}的「{SPEC["field_labels"][field]}」需为 {minimum} 至 {_MAX_SEQUENCE} 的整数')
    return value


def _content(row, operation, label):
    value = row.get('content')
    if operation == 'delete':
        if value != '':
            raise ValueError(f'{label}的撤牌内容须为空字符串')
        return
    if not isinstance(value, str) or not value.strip() or len(value) > 240:
        raise ValueError(f'{label}的牌面内容需为 1–240 字符的非空正文')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        raise ValueError(f'{label}的牌面内容包含无效的 Unicode 字符') from None


def validate(payload):
    if not isinstance(payload, dict):
        raise ValueError('回放输入需为对象')
    offsets = {}
    for index, row in enumerate(_rows(payload, 'checkpoints', 100), 1):
        label = f'起始位点第 {index} 行'
        stream = _identifier(row, 'stream', label)
        if stream in offsets:
            raise ValueError(f'{label}的流 {stream} 已登记起始位点')
        offsets[stream] = _number(row, 'sequence', 0, label)
    if not offsets:
        raise ValueError('至少登记一条流的起始位点')

    sign_keys = set()
    for index, row in enumerate(_rows(payload, 'snapshot', 1000), 1):
        label = f'牌面快照第 {index} 行'
        stream = _identifier(row, 'stream', label)
        sign_id = _identifier(row, 'sign_id', label)
        sequence = _number(row, 'sequence', 1, label)
        _content(row, 'put', label)
        if stream not in offsets or sequence > offsets[stream]:
            raise ValueError(f'{label}的序号须处在已登记流的起始位点内')
        if (stream, sign_id) in sign_keys:
            raise ValueError(f'{label}的牌面 {sign_id} 在流 {stream} 中已登记')
        sign_keys.add((stream, sign_id))

    for index, row in enumerate(_rows(payload, 'events', 1000), 1):
        label = f'到货事件第 {index} 行'
        stream = _identifier(row, 'stream', label)
        if stream not in offsets:
            raise ValueError(f'{label}的流 {stream} 缺少起始位点')
        _identifier(row, 'event_id', label)
        _identifier(row, 'sign_id', label)
        _number(row, 'batch', 1, label)
        _number(row, 'sequence', 1, label)
        operation = row.get('operation')
        if operation not in ('put', 'delete'):
            raise ValueError(f'{label}的动作须为 put 或 delete')
        _content(row, operation, label)

    _validate_adjudications(payload, offsets)


def _validate_adjudications(payload, offsets):
    rows = payload.get('adjudications')
    if rows is None:
        return
    if not isinstance(rows, list) or len(rows) > 100 or any(not isinstance(row, dict) for row in rows):
        raise ValueError('「上游补交裁定」需为最多 100 行的对象数组')
    ruling_ids = set()
    for index, row in enumerate(rows, 1):
        label = f'补交裁定第 {index} 行'
        ruling_id = _identifier(row, 'ruling_id', label)
        if ruling_id in ruling_ids:
            raise ValueError(f'{label}的裁定编号 {ruling_id} 重复')
        ruling_ids.add(ruling_id)
        stream = _identifier(row, 'stream', label)
        if stream not in offsets:
            raise ValueError(f'{label}的流 {stream} 缺少起始位点')
        _number(row, 'batch', 1, label)
        _number(row, 'sequence', 1, label)
        _optional_identifier(row, 'event_id', label)
        selected = row.get('selected_receipts')
        if not isinstance(selected, list) or any(type(item) is not int or item < 1 for item in selected):
            raise ValueError(f'{label}的选中接收行需为正整数数组；缺号挂起请显式给空数组并附完整裁定信封')
        if len(set(selected)) != len(selected):
            raise ValueError(f'{label}的选中接收行存在重复')
        envelope = [row.get(field) is not None for field in _RULING_FIELDS]
        if not selected:
            if not all(envelope):
                raise ValueError(f'{label}为缺号挂起裁定，必须给出牌面编号、动作与正文组成的完整信封')
            if row.get('operation') not in ('put', 'delete'):
                raise ValueError(f'{label}的动作须为 put 或 delete')
            _content(row, row['operation'], label)
        elif any(envelope):
            raise ValueError(f'{label}选中接收行时整组以原信封为准，不得再附带裁定信封')


def _stage(conn, event, offset):
    """Register one arrival. Returns the duplicate descriptor when redelivered."""
    matches = store.journal_matches(conn, event)
    if matches and all(row[field] == event[field] for field in _ENVELOPE for row in matches):
        return {**event, 'state': 'duplicate',
                'reason': f'与接收行 {matches[0]["receipt"]} 的事件信封完全一致'}
    if matches:
        locked = [row for row in matches if row['ruling_id'] is not None]
        if locked:
            # Selected records cannot clash again: the new version is quarantined on its own.
            store.insert_receipt(conn, event, 'conflict',
                                 f'接收行 {locked[0]["receipt"]} 已被裁定 {locked[0]["ruling_id"]} 选中，撞号版本单独隔离')
        else:
            store.quarantine_pending(conn, event)
            store.insert_receipt(conn, event, 'conflict',
                                 f'与接收行 {matches[0]["receipt"]} 的事件序号或事件编号重叠，信封内容不同')
    elif event['seq'] <= offset:
        store.insert_receipt(conn, event, 'history', '序号处在起始位点内，缺少原始事件信封供核对')
    else:
        _resolve_held_rulings(conn, event)


def _resolve_held_rulings(conn, event):
    """A missing-number record has arrived: held rulings take effect or lapse."""
    held = [row for row in store.seq_rulings(conn, event['stream'], event['seq']) if row['state'] == 'held']
    if not held:
        store.insert_receipt(conn, event, 'pending', '')
        return
    envelope_matches = all(row[field] == event[field] for field in _ENVELOPE for row in held)
    if envelope_matches:
        ruling = held[0]
        store.insert_receipt(conn, event, 'adjudicated',
                             f'与挂起裁定 {ruling["ruling_id"]} 的信封一致，裁定生效', ruling['ruling_id'])
        for row in held:
            store.update_ruling(conn, row['ruling_id'], 'effective',
                                f'缺号已到，接收行 {event["receipt"]} 与裁定信封一致')
    else:
        reason = f'缺号 {event["seq"]} 等号期间收到不同版本（接收行 {event["receipt"]}），原裁定失效'
        store.lapse_held(conn, event['stream'], event['seq'], reason)
        store.insert_receipt(conn, event, 'pending', '')


def _implicated_group(conn, stream, seq, event_id):
    """Transitive closure over receipts sharing a sequence or an event id."""
    group = set()
    frontier = [(seq, event_id)]
    seen = set()
    while frontier:
        current_seq, current_event = frontier.pop()
        key = (current_seq, current_event)
        if key in seen:
            continue
        seen.add(key)
        rows = conn.execute(text('''
            SELECT receipt, seq, event_id FROM marquee_cdc_journal
            WHERE stream = :stream AND state <> 'history'
              AND (seq = :seq OR (event_id = :event_id AND :event_id IS NOT NULL))
        '''), {'stream': stream, 'seq': current_seq, 'event_id': current_event}).mappings().all()
        for row in rows:
            if row['receipt'] not in group:
                group.add(row['receipt'])
                frontier.append((row['seq'], row['event_id']))
    return group


def _register_ruling(conn, ruling, offset):
    """Validate and register one upstream ruling within the batch transaction."""
    stream, seq = ruling['stream'], ruling['seq']
    if store.get_ruling(conn, ruling['ruling_id']) is not None:
        raise ValueError(f'裁定编号 {ruling["ruling_id"]} 重复')
    open_ruling = store.seq_rulings(conn, stream, seq)
    if open_ruling and any(row['state'] == 'held' for row in open_ruling):
        store.insert_ruling(conn, ruling, 'rejected', f'序号 {seq} 已有挂起待号裁定，需等其生效或失效')
        return
    if not ruling['selected_receipts']:
        if seq <= offset:
            # The missing number has already been filled by another path.
            store.insert_ruling(conn, ruling, 'rejected', f'序号 {seq} 已提交，缺号裁定无需再生效')
        else:
            store.insert_ruling(conn, ruling, 'held', f'缺号 {seq} 先挂起裁定，等真实接收行到达核对')
        return
    rows = conn.execute(text('''
        SELECT receipt, seq, state, ruling_id FROM marquee_cdc_journal
        WHERE stream = :stream AND receipt IN :receipts
    ''').bindparams(bindparam('receipts', expanding=True)),
        {'stream': stream, 'receipts': ruling['selected_receipts']}).mappings().all()
    found = {row['receipt']: row for row in rows}
    missing = [item for item in ruling['selected_receipts'] if item not in found]
    if missing:
        store.insert_ruling(conn, ruling, 'rejected', f'接收行 {",".join(map(str, missing))} 不存在或不属于该流')
        return
    if any(row['ruling_id'] is not None for row in found.values()):
        store.insert_ruling(conn, ruling, 'rejected', '选中记录已被其他裁定锁定，不可重复撞号')
        return
    lapsed = conn.execute(text('''
        SELECT ruling_id FROM marquee_cdc_adjudications
        WHERE stream = :stream AND seq = :seq AND state = 'lapsed'
    '''), {'stream': stream, 'seq': seq}).mappings().all()
    selected_applied = [item for item in ruling['selected_receipts'] if found[item]['state'] == 'applied']
    if selected_applied:
        if lapsed:
            # 缺号挂起裁定失效后，真实补号已落定：补裁定只确认既有结果，不回改牌面与位点。
            store.insert_ruling(
                conn, ruling, 'released',
                f'原裁定 {",".join(row["ruling_id"] for row in lapsed)} 已失效；选中接收行 '
                f'{",".join(map(str, sorted(selected_applied)))} 已提交，补裁定确认既有落定结果，不改写已提交变更')
            return
        store.insert_ruling(conn, ruling, 'rejected',
                            f'选中接收行 {",".join(map(str, sorted(selected_applied)))} 已提交，补裁定不改写已提交变更')
        return
    group = _implicated_group(conn, stream, seq, ruling.get('event_id'))
    if not group.issuperset(ruling['selected_receipts']):
        store.insert_ruling(conn, ruling, 'rejected', '选中记录与裁定序号或事件号不属同一整组')
        return
    group_rows = conn.execute(text('''
        SELECT receipt, state FROM marquee_cdc_journal
        WHERE stream = :stream AND receipt IN :receipts
    ''').bindparams(bindparam('receipts', expanding=True)),
        {'stream': stream, 'receipts': list(group)}).mappings().all()
    submitted = {row['receipt'] for row in group_rows if row['state'] in ('applied', 'history')}
    if submitted:
        # Other implicated records already landed (e.g. an older event of the same
        # sequence); the ruling still pins the remaining versions of the group.
        reason_suffix = f'；组内已提交行 {",".join(map(str, sorted(submitted)))} 保持不变'
    else:
        reason_suffix = ''
    lockable = sorted(group - submitted)
    store.mark_adjudicated(conn, stream, lockable, ruling['ruling_id'],
                           f'整组裁定 {ruling["ruling_id"]} 选中接收行 {",".join(map(str, ruling["selected_receipts"]))}')
    store.insert_ruling(conn, ruling, 'effective',
                        f'牵连接收行 {",".join(map(str, sorted(group)))} 已整组锁定，待位点抵达后放行{reason_suffix}')


def _blocker(waiting, offset):
    if not waiting:
        return '已追平', '', '', '收到的连续事件已处理'
    first = waiting[0]
    if first['seq'] > offset + 1:
        return '缺号待补', offset + 1, first['seq'] - 1, f'已收到后续序号 {first["seq"]}'
    if first['state'] == 'pending':
        return '矛盾待核', first['seq'], first['seq'], f'核对接收行 {first["receipt"]} 的事件信封'
    if first['state'] == 'conflict':
        # A quarantined envelope sits exactly at the next sequence: the stream
        # is blocked on the conflict and cannot be advanced past it.
        return '矛盾待核', first['seq'], first['seq'], f'核对接收行 {first["receipt"]} 的事件信封'
    if first['state'] == 'adjudicated':
        return '裁定待放行', first['seq'], first['seq'], f'按裁定 {first["ruling_id"]} 整组放行'
    return '已追平', '', '', '收到的连续事件已处理'


def _offset_row(stream, offset, waiting):
    state, start, end, detail = _blocker(waiting, offset)
    return {'分区流': stream, '消费位点': offset, '续放状态': state,
            '阻塞起号': start, '阻塞止号': end, '依据': detail}


def _consume_stream(conn, stream, offsets):
    previous = offset = offsets[stream]
    waiting = store.waiting_events(conn, stream, offset)
    for event in waiting:
        if event['seq'] != offset + 1 or event['state'] not in ('pending', 'adjudicated'):
            break
        store.apply_event(conn, event)
        offset = event['seq']
        if event['state'] == 'adjudicated':
            ruling = store.get_ruling(conn, event['ruling_id'])
            selected = [int(item) for item in ruling['selected_receipts'].split(',') if item]
            if store.ruling_remaining(conn, selected) == 0 and ruling['state'] == 'effective':
                store.update_ruling(conn, event['ruling_id'], 'released',
                                    f'整组随位点 {offset} 与牌面投影同事务落定')
    if offset != previous:
        store.advance_offset(conn, stream, offset)
    return {'原位点': previous, '提交事件数': offset - previous,
            **_offset_row(stream, offset, waiting[offset - previous:])}


def _reconcile_held_rulings(conn, streams):
    """Match held rulings against arrivals registered earlier in the same batch."""
    for stream in streams:
        held_rows = conn.execute(text('''
            SELECT * FROM marquee_cdc_adjudications
            WHERE stream = :stream AND state = 'held'
        '''), {'stream': stream}).mappings().all()
        for ruling in held_rows:
            arrival = conn.execute(text('''
                SELECT * FROM marquee_cdc_journal
                WHERE stream = :stream AND seq = :seq AND state IN ('pending', 'conflict')
                ORDER BY receipt LIMIT 1
            '''), {'stream': stream, 'seq': ruling['seq']}).mappings().first()
            if arrival is None:
                continue
            same_envelope = all(arrival[field] == ruling[field] for field in _RULING_FIELDS)
            if same_envelope:
                conn.execute(text('''
                    UPDATE marquee_cdc_journal
                    SET state = 'adjudicated', ruling_id = :ruling_id,
                        reason = :reason
                    WHERE receipt = :receipt
                '''), {'ruling_id': ruling['ruling_id'], 'receipt': arrival['receipt'],
                       'reason': f'与挂起裁定 {ruling["ruling_id"]} 的信封一致，裁定生效'})
                store.update_ruling(conn, ruling['ruling_id'], 'effective',
                                    f'缺号已到，接收行 {arrival["receipt"]} 与裁定信封一致')
            else:
                store.lapse_held(
                    conn, stream, ruling['seq'],
                    f'缺号 {ruling["seq"]} 等号期间收到不同版本（接收行 {arrival["receipt"]}），原裁定失效')


def _apply_batch(conn, batch_no, arrivals, rulings):
    duplicates = []
    progress = []
    with conn.begin():
        offsets = store.read_offsets(conn)
        # Every arrival of the batch is registered before consumption, so
        # concurrent deliveries in one batch see each other's envelopes.
        for event in arrivals:
            duplicate = _stage(conn, event, offsets[event['stream']])
            if duplicate is not None:
                duplicates.append(duplicate)
        for ruling in rulings:
            _register_ruling(conn, ruling, offsets[ruling['stream']])
        _reconcile_held_rulings(conn, sorted({event['stream'] for event in arrivals}
                                             | {ruling['stream'] for ruling in rulings}))
        touched = sorted({event['stream'] for event in arrivals}
                         | {ruling['stream'] for ruling in rulings})
        for stream in touched:
            row = _consume_stream(conn, stream, offsets)
            progress.append({'到货批号': batch_no, **row})
    return duplicates, progress


def _arrival_events(rows):
    return sorted([
        dict(receipt=index, batch_no=row['batch'], stream=row['stream'], seq=row['sequence'],
             event_id=row['event_id'], sign_id=row['sign_id'], operation=row['operation'], content=row['content'])
        for index, row in enumerate(rows, 1)
    ], key=itemgetter('batch_no'))


def _arrival_rulings(rows):
    result = []
    for index, row in enumerate(rows or [], 1):
        result.append(dict(ruling_id=row['ruling_id'], batch_no=row['batch'], stream=row['stream'],
                           seq=row['sequence'], event_id=row.get('event_id'),
                           selected_receipts=list(row['selected_receipts']),
                           sign_id=row.get('sign_id'), operation=row.get('operation'),
                           content=row.get('content')))
    return sorted(result, key=itemgetter('batch_no'))


def _batched(items, key):
    return groupby(items, key=itemgetter(key))


def _event_row(event):
    state = event['state']
    return {'接收行': event['receipt'], '到货批号': event['batch_no'], '分区流': event['stream'],
            '序号': event['seq'], '事件编号': event['event_id'], '牌面编号': event['sign_id'],
            '动作': _OPERATIONS[event['operation']], '内容': event['content'],
            '状态': '重投忽略' if state == 'duplicate' else _STATES[state],
            '裁定编号': event.get('ruling_id') or '',
            '依据': event['reason'] or ('已随位点提交' if state == 'applied' else '等待本流解除前序阻塞')}


def _ruling_row(row):
    return {'裁定编号': row['ruling_id'], '到货批号': row['batch_no'], '分区流': row['stream'],
            '序号': row['seq'], '事件编号': row['event_id'] or '',
            '选中接收行': row['selected_receipts'], '状态': _RULING_STATES[row['state']], '依据': row['reason']}


def solve(payload, engine):
    validate(payload)
    duplicates = []
    progress = []
    arrivals = _arrival_events(payload['events'])
    rulings = _arrival_rulings(payload.get('adjudications'))
    with store.isolated_replay(engine) as conn:
        store.initialize(conn, payload['checkpoints'], payload['snapshot'])
        batch_numbers = sorted({row['batch_no'] for row in arrivals} | {row['batch_no'] for row in rulings})
        arrival_groups = {key: list(items) for key, items in _batched(arrivals, 'batch_no')}
        ruling_groups = {key: list(items) for key, items in _batched(rulings, 'batch_no')}
        for batch_no in batch_numbers:
            repeated, committed = _apply_batch(
                conn, batch_no, arrival_groups.get(batch_no, []), ruling_groups.get(batch_no, []))
            duplicates.extend(repeated)
            progress.extend(committed)
        with conn.begin():
            offsets = [_offset_row(stream, offset, store.waiting_events(conn, stream, offset))
                       for stream, offset in store.read_offsets(conn).items()]
            signs = [{'分区流': row['stream'], '牌面编号': row['sign_id'],
                      '当前内容': row['content'], '来源序号': row['seq']} for row in store.read_projection(conn)]
            journal = store.read_journal(conn)
            ruling_rows = store.read_rulings(conn)
    events = [_event_row(event) for event in sorted([*journal, *duplicates], key=itemgetter('batch_no', 'receipt'))]
    released = [_ruling_row(row) for row in ruling_rows if row['state'] == 'released']
    metrics = {
        '已提交事件数': sum(row['state'] == 'applied' for row in journal),
        '重投忽略数': len(duplicates),
        '矛盾隔离事件数': sum(row['state'] == 'conflict' for row in journal),
        '历史未核实数': sum(row['state'] == 'history' for row in journal),
        '暂缓事件数': sum(row['state'] == 'pending' for row in journal),
        '缺口流数': sum(row['续放状态'] == '缺号待补' for row in offsets),
        '矛盾阻塞流数': sum(row['续放状态'] == '矛盾待核' for row in offsets),
        '在用牌面数': len(signs),
        '裁定挂起数': sum(row['state'] == 'held' for row in ruling_rows),
        '裁定生效数': sum(row['state'] == 'effective' for row in ruling_rows),
        '裁定放行数': sum(row['state'] == 'released' for row in ruling_rows),
        '裁定失效数': sum(row['state'] == 'lapsed' for row in ruling_rows),
        '裁定驳回数': sum(row['state'] == 'rejected' for row in ruling_rows),
    }
    return {
        'summary': (f'本轮提交 {metrics["已提交事件数"]} 条变更，发布单有 {len(signs)} 块在用牌面。'
                    f'裁定放行 {metrics["裁定放行数"]} 组、失效 {metrics["裁定失效数"]} 组、驳回 {metrics["裁定驳回数"]} 组。'
                    f'{metrics["缺口流数"]} 条流待补号，{metrics["矛盾阻塞流数"]} 条流待核对矛盾事件。'),
        'metrics': metrics,
        'sections': [
            {'title': '牌面发布单', 'rows': signs},
            {'title': '消费位点与首个阻塞', 'rows': offsets},
            {'title': '裁定状态', 'rows': [_ruling_row(row) for row in ruling_rows]},
            {'title': '已整组放行记录', 'rows': released},
            {'title': '分批推进记录', 'rows': progress},
            {'title': '事件接收与处置', 'rows': events},
        ],
    }
