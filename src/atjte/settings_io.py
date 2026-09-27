"""Read/write helpers for a strategy's ``strategy_settings.py`` and ``trading_sessions.py``.

The control-panel GUI uses these to render an editable form and write changes back. Both files
are hand-maintained Python with meaningful inline comments, so edits are *surgical*: we locate
each assignment with the ``ast`` module and replace only the value text, leaving comments,
spacing and unrelated lines untouched. Settings values are restricted to Python literals
(``ast.literal_eval``) so saving the form can never inject executable code into a file the bots
import at startup.
"""

import ast
import io
import os
import tokenize
from datetime import time as _time

_DAYS = ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')


# ── file io ──────────────────────────────────────────────────────────────────

def _read(path):
    with open(path, 'r', encoding='utf-8') as fh:
        return fh.read()


def _write(path, text):
    # newline='' keeps our explicit '\n' joins from being doubled on Windows.
    with open(path, 'w', encoding='utf-8', newline='') as fh:
        fh.write(text)


def _node_source(lines, node):
    """Exact source text of an AST node, using its 1-based line / 0-based col spans."""
    if node.lineno == node.end_lineno:
        return lines[node.lineno - 1][node.col_offset:node.end_col_offset]
    parts = [lines[node.lineno - 1][node.col_offset:]]
    parts.extend(lines[node.lineno:node.end_lineno - 1])
    parts.append(lines[node.end_lineno - 1][:node.end_col_offset])
    return '\n'.join(parts)


def _inline_comments(src):
    """Map 1-based line number -> inline comment text (without the leading ``#``)."""
    out = {}
    try:
        for tok in tokenize.generate_tokens(io.StringIO(src).readline):
            if tok.type == tokenize.COMMENT:
                out[tok.start[0]] = tok.string.lstrip('#').strip()
    except tokenize.TokenError:
        pass
    return out


# ── strategy_settings.py ─────────────────────────────────────────────────────

def read_settings(path):
    """Return an ordered list of ``{name, raw, display, is_str, comment, editable}`` for each
    top-level constant assignment. ``raw`` is the value's source text; ``display`` is what the GUI
    shows in the entry box — for string values it's the bare text without the surrounding quotes,
    otherwise it's identical to ``raw``. ``is_str`` flags string values (the GUI re-adds the quotes
    on save). ``editable`` is False for values that aren't plain literals (e.g. an alias like
    ``X = SOME_OTHER_CONST``)."""
    src = _read(path)
    lines = src.split('\n')
    comments = _inline_comments(src)
    out = []
    for node in ast.parse(src).body:
        if not (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)):
            continue
        name = node.targets[0].id
        if name.startswith('_'):
            continue
        raw = _node_source(lines, node.value).strip()
        try:
            value = ast.literal_eval(node.value)
            editable = True
        except (ValueError, SyntaxError, TypeError):
            value = None
            editable = False
        is_str = editable and isinstance(value, str)
        out.append({
            'name': name,
            'raw': raw,
            'display': value if is_str else raw,
            'is_str': is_str,
            'comment': comments.get(node.lineno, ''),
            'editable': editable,
        })
    return out


def read_values(path):
    """Return ``{name: value}`` for every top-level literal constant in the settings file.

    A convenience over :func:`read_settings` for callers that just want the evaluated values
    (e.g. the overview table showing leverage / allocation). Non-literal assignments are skipped;
    a missing or unparseable file yields an empty dict rather than raising, so a display caller
    never crashes on a malformed config."""
    out = {}
    try:
        for item in read_settings(path):
            if item['editable']:
                out[item['name']] = ast.literal_eval(item['raw'])
    except (OSError, SyntaxError, ValueError):
        pass
    return out


def validate_literal(raw):
    """Parse ``raw`` as a Python literal; return the value or raise ValueError with a message."""
    try:
        return ast.literal_eval(raw)
    except (ValueError, SyntaxError, TypeError) as exc:
        raise ValueError(f'not a valid Python literal: {exc}') from exc


# Settings written as a FRACTION in [0, 1] (e.g. 0.25 = 25%, 0.8 = 80%), NOT a 0-100 percent.
# The unified convention across strategy_settings.py; the editor hard-blocks a save that puts any
# of these outside [0, 1], catching the classic "typed 80 instead of 0.8" mistake before it reaches
# a running bot. Add a new percentage setting here and it's validated automatically.
PERCENT_FIELDS = ('ALLOCATION_PERC', 'SIGNAL_HOLD_BUFFER_PCT', 'MIN_MARGIN_PERC_MT5')


def check_percent_ranges(values):
    """Return ``[(name, value), ...]`` for PERCENT_FIELDS that fall outside [0, 1].

    A HARD save-time gate (unlike :func:`check_settings`, which only warns): these are fractions,
    so a value above 1 is a units mistake — almost never a deliberate config — and silently passing
    e.g. MIN_MARGIN_PERC_MT5 = 80 would make the bot compute an 8000% halt threshold and never
    trade. Non-numeric / missing values are skipped so a partial mapping can't raise."""
    bad = []
    for name in PERCENT_FIELDS:
        v = values.get(name)
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            continue
        if not 0 <= v <= 1:
            bad.append((name, v))
    return bad


def check_settings(values):
    """Sanity-check a whole ``{name: value}`` settings mapping for *likely mistakes*.

    ``values`` maps each setting name to its already-evaluated Python value. Returns a list of
    human-readable warning strings — one per suspicious value or relationship — or an empty list
    when nothing looks wrong. These are SOFT checks: the GUI surfaces them as a "this seems wrong,
    proceed anyway?" prompt and never hard-blocks a save, so a deliberately unusual config is still
    allowed. Any setting that's missing from ``values`` or isn't a plain number is skipped (never
    guessed at), so a partial mapping can't raise or raise false alarms.

    The spread-threshold ladder is read straight off the bot's own entry/exit logic (bot_core):
    a long enters when avg_bid <= BUY_ENTRY and exits when avg_ask >= BUY_EXIT; a short enters
    when avg_ask >= SELL_ENTRY and exits when avg_bid <= SELL_EXIT. For each round trip to make
    money the exit spread must beat the entry spread, which forces the ladder
        SELL_ENTRY  >  BUY_EXIT   >  BUY_ENTRY      and
        SELL_ENTRY  >  SELL_EXIT  >  BUY_ENTRY
    i.e. SELL_ENTRY at the top, BUY_ENTRY at the bottom.
    """
    warnings = []

    def num(name):
        """The value of ``name`` if it's a real number, else None (bools are NOT numbers here)."""
        v = values.get(name)
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return None
        return v

    # ── spread threshold ladder ──────────────────────────────────────────────
    be, bx = num('LIMIT_PRICE_BUY_ENTRY'), num('LIMIT_PRICE_BUY_EXIT')
    se, sx = num('LIMIT_PRICE_SELL_ENTRY'), num('LIMIT_PRICE_SELL_EXIT')
    if bx is not None and be is not None and bx <= be:
        warnings.append(
            f'LIMIT_PRICE_BUY_EXIT ({bx}) is not above LIMIT_PRICE_BUY_ENTRY ({be}). '
            'A long would exit at a spread no better than it entered — the cycle loses on every fill.')
    if se is not None and sx is not None and se <= sx:
        warnings.append(
            f'LIMIT_PRICE_SELL_ENTRY ({se}) is not above LIMIT_PRICE_SELL_EXIT ({sx}). '
            'A short would exit at a spread no better than it entered — the cycle loses on every fill.')
    if se is not None and bx is not None and se <= bx:
        warnings.append(
            f'LIMIT_PRICE_SELL_ENTRY ({se}) should be above LIMIT_PRICE_BUY_EXIT ({bx}) '
            '(sell-entry sits at the top of the ladder).')
    if sx is not None and be is not None and sx <= be:
        warnings.append(
            f'LIMIT_PRICE_SELL_EXIT ({sx}) should be above LIMIT_PRICE_BUY_ENTRY ({be}) '
            '(buy-entry sits at the bottom of the ladder).')

    # ── order size & exposure caps ───────────────────────────────────────────
    vol = num('VOLUME_HL')
    long_cap = num('MAX_EXPOSURE_HL')      # None (no hard cap) is skipped by num()
    short_cap = num('MIN_EXPOSURE_HL')
    if vol is not None and vol <= 0:
        warnings.append(f'VOLUME_HL ({vol}) should be a positive order size.')
    if long_cap is not None and long_cap < 0:
        warnings.append(f'MAX_EXPOSURE_HL ({long_cap}) should be >= 0 — it caps LONG exposure.')
    if short_cap is not None and short_cap > 0:
        warnings.append(
            f'MIN_EXPOSURE_HL ({short_cap}) should be <= 0 — it caps SHORT exposure, '
            'so it must be zero or negative.')
    if vol and vol > 0 and long_cap is not None and long_cap > 0 and vol > long_cap:
        warnings.append(
            f'VOLUME_HL ({vol}) is larger than MAX_EXPOSURE_HL ({long_cap}); '
            'a single long clip already exceeds the hard cap.')
    if vol and vol > 0 and short_cap is not None and short_cap < 0 and vol > abs(short_cap):
        warnings.append(
            f'VOLUME_HL ({vol}) is larger than |MIN_EXPOSURE_HL| ({short_cap}); '
            'a single short clip already exceeds the hard cap.')

    # ── misc ranges ──────────────────────────────────────────────────────────
    lev = num('HL_LEVERAGE')
    if lev is not None and lev <= 0:
        warnings.append(f'HL_LEVERAGE ({lev}) should be a positive multiplier.')
    # NB: 0-1 range for percentage fields (ALLOCATION_PERC, SIGNAL_HOLD_BUFFER_PCT,
    # MIN_MARGIN_PERC_MT5, ...) is HARD-enforced separately via check_percent_ranges(), so it's
    # not repeated here as a soft warning.
    off = num('MIN_TOB_OFFSET')
    if off is not None and off < 0:
        warnings.append(f'MIN_TOB_OFFSET ({off}) should not be negative.')
    amend = num('AMEND_INTERVAL')
    if amend is not None and amend <= 0:
        warnings.append(f'AMEND_INTERVAL ({amend}) should be a positive number of seconds.')
    sw = num('SPREAD_WINDOW_S')
    if sw is not None and sw < 1:
        warnings.append(f'SPREAD_WINDOW_S ({sw}) should be at least 1 second.')
    qw = num('QUOTE_WINDOW_S')
    if qw is not None and qw < 1:
        warnings.append(f'QUOTE_WINDOW_S ({qw}) should be at least 1 second.')
    mdl = num('MAX_DAILY_LOSS')
    if mdl is not None and mdl < 0:
        warnings.append(
            f'MAX_DAILY_LOSS ({mdl}) should be >= 0 — give it as a positive USD amount (0 disables it).')

    return warnings


def write_settings(path, changes):
    """Apply ``{name: new_raw}`` edits in place, preserving comments and layout.

    Every new value is validated as a literal first; an unknown name or a non-literal value
    raises before anything is written, so the file is never left half-edited."""
    if not changes:
        return
    for name, raw in changes.items():
        validate_literal(raw)

    src = _read(path)
    lines = src.split('\n')
    nodes = {}
    for node in ast.parse(src).body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id in changes):
            nodes[node.targets[0].id] = node.value

    missing = set(changes) - set(nodes)
    if missing:
        raise KeyError(f'settings not found in {path}: {sorted(missing)}')

    # Apply bottom-to-top so earlier (higher) spans keep their offsets as we rewrite.
    for name, vnode in sorted(nodes.items(), key=lambda kv: kv[1].lineno, reverse=True):
        new_raw = changes[name]
        if vnode.lineno == vnode.end_lineno:
            line = lines[vnode.lineno - 1]
            lines[vnode.lineno - 1] = line[:vnode.col_offset] + new_raw + line[vnode.end_col_offset:]
        else:
            head = lines[vnode.lineno - 1][:vnode.col_offset]
            tail = lines[vnode.end_lineno - 1][vnode.end_col_offset:]
            lines[vnode.lineno - 1] = head + new_raw + tail
            del lines[vnode.lineno:vnode.end_lineno]

    _write(path, '\n'.join(lines))


def append_settings(path, additions):
    """Append constants that don't yet exist in the file (used to materialise settings an old
    strategy is missing, with the template default).

    ``additions`` maps ``name -> (raw, comment)``: ``raw`` is the literal source text to write,
    ``comment`` an optional inline note. Every value is validated as a literal first, and a name that
    already exists raises (callers route those through :func:`write_settings` instead), so the file is
    never left half-edited. New lines go under a one-time ``# --- added by control panel ---`` header."""
    if not additions:
        return
    for _name, (raw, _comment) in additions.items():
        validate_literal(raw)

    src = _read(path)
    existing = {node.targets[0].id for node in ast.parse(src).body
                if isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)}
    clash = set(additions) & existing
    if clash:
        raise KeyError(f'settings already present in {path}: {sorted(clash)}')

    header = '# --- added by control panel ---'
    parts = [src]
    if not src.endswith('\n'):
        parts.append('\n')
    if header not in src:
        parts.append('\n' + header + '\n')
    for name, (raw, comment) in additions.items():
        line = f'{name} = {raw}'
        if comment:
            line += f'  # {comment}'
        parts.append(line + '\n')
    _write(path, ''.join(parts))


# ── trading_sessions.py ──────────────────────────────────────────────────────

def _parse_time_call(node):
    """``time(h, m, s)`` AST Call -> (h, m, s) ints (missing args default to 0)."""
    args = [a.value if isinstance(a, ast.Constant) else 0 for a in node.args]
    args = (args + [0, 0, 0])[:3]
    return tuple(int(a) for a in args)


def _parse_sessions_dict(node):
    out = {}
    for key, val in zip(node.keys, node.values):
        if not isinstance(key, ast.Constant):
            continue
        day = key.value
        if isinstance(val, ast.Constant) and val.value is None:
            out[day] = None
        elif isinstance(val, ast.Dict):
            window = {}
            for k2, v2 in zip(val.keys, val.values):
                if isinstance(k2, ast.Constant) and isinstance(v2, ast.Call):
                    window[k2.value] = _parse_time_call(v2)
            out[day] = {'start': window.get('start', (0, 0, 0)),
                        'end': window.get('end', (0, 0, 0))}
    return out


def read_sessions(path):
    """Return ``{timezone: str, sessions: {day: None | {'start': (h,m,s), 'end': (h,m,s)}}}``."""
    src = _read(path)
    timezone = None
    sessions = {}
    for node in ast.parse(src).body:
        if not (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)):
            continue
        name = node.targets[0].id
        if name == 'TIMEZONE' and isinstance(node.value, ast.Constant):
            timezone = node.value.value
        elif name == 'TRADING_SESSIONS' and isinstance(node.value, ast.Dict):
            sessions = _parse_sessions_dict(node.value)
    # Normalise to the full week in display order.
    sessions = {d: sessions.get(d) for d in _DAYS}
    return {'timezone': timezone, 'sessions': sessions}


def _fmt_window(window):
    if window is None:
        return 'None'
    s, e = window['start'], window['end']
    return (f"{{'start': time({s[0]}, {s[1]}, {s[2]}), "
            f"'end': time({e[0]}, {e[1]}, {e[2]})}}")


def write_sessions(path, timezone, sessions):
    """Rewrite ``TIMEZONE`` and the whole ``TRADING_SESSIONS`` block, preserving everything else.

    ``sessions`` is ``{day: None | {'start': (h,m,s), 'end': (h,m,s)}}``. The TIMEZONE value is
    replaced in place (keeping its inline comment); TRADING_SESSIONS is regenerated as a fresh
    literal block, since its contents are fully described by ``sessions``."""
    src = _read(path)
    lines = src.split('\n')
    tz_node = None
    sess_node = None
    for node in ast.parse(src).body:
        if not (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)):
            continue
        name = node.targets[0].id
        if name == 'TIMEZONE':
            tz_node = node
        elif name == 'TRADING_SESSIONS':
            sess_node = node
    if tz_node is None or sess_node is None:
        raise KeyError(f'TIMEZONE / TRADING_SESSIONS not found in {path}')

    # 1) Replace the TRADING_SESSIONS statement (multi-line) first — it sits below TIMEZONE,
    #    so rewriting it doesn't disturb TIMEZONE's line index.
    block = ['TRADING_SESSIONS = {']
    for day in _DAYS:
        block.append(f"    '{day}': {_fmt_window(sessions.get(day))},")
    block.append('}')
    lines[sess_node.lineno - 1:sess_node.end_lineno] = block

    # 2) Replace the TIMEZONE value in place, keeping any trailing comment.
    v = tz_node.value
    line = lines[v.lineno - 1]
    lines[v.lineno - 1] = line[:v.col_offset] + repr(timezone) + line[v.end_col_offset:]

    _write(path, '\n'.join(lines))


def available_timezones():
    """Sorted list of IANA timezone names, for the GUI's timezone pickers. Returns an empty list if
    the tz database can't be enumerated (e.g. no ``tzdata``), so the picker degrades to free text."""
    try:
        from zoneinfo import available_timezones as _avail
        return sorted(_avail())
    except Exception:
        return []


def validate_timezone(name):
    """Raise ValueError if ``name`` is not a usable IANA zone (zoneinfo can resolve it)."""
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError) as exc:
        raise ValueError(f'unknown timezone {name!r}: {exc}') from exc


def validate_hms(text):
    """Parse 'H:M:S' or 'H:M' -> (h, m, s); raise ValueError on bad input."""
    parts = [p.strip() for p in str(text).split(':')]
    if not 2 <= len(parts) <= 3:
        raise ValueError(f'expected H:M or H:M:S, got {text!r}')
    try:
        nums = [int(p) for p in parts]
    except ValueError as exc:
        raise ValueError(f'non-numeric time field in {text!r}') from exc
    nums = (nums + [0])[:3]
    h, m, s = nums
    try:
        _time(h, m, s)  # range-checks the fields
    except ValueError as exc:
        raise ValueError(f'time out of range in {text!r}: {exc}') from exc
    return (h, m, s)
