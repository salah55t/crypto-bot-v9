#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
اختبارات V9.30.0 — اتساق الصفقات (تلغرام يقول مغلقة واللوحة تقول مفتوحة؟) + البقاء مستيقظًا
1) قفل اتصال DB المشترك (db_conn_lock) يغلف كل مواقع الكتابة
2) _cache_touch_open: لا إحياء لصفقة مغلقة في الكاش
3) _reconcile_cache_with_db: مصالحة ثنائية الاتجاه (حذف المغلقة / تحميل اليتيمة)
4) إصلاح السقوط بعد الإغلاق في trade_management_loop (journey_completed/take_profit)
5) تحقق ما بعد الإثبات في close_signal
6) نبضة البقاء الذاتية (keep_alive_loop)
"""
import sys, os, re, inspect, threading, types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402
vnz.logger.disabled = True

PASS, FAIL = [], []

def check(name: str, cond: bool, detail: str = ''):
    (PASS if cond else FAIL).append(name)
    print(f"{'✅' if cond else '❌'} {name}" + (f" — {detail}" if detail and not cond else ''))

SRC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'vnz.py'), encoding='utf-8').read()

def _enclosing_lock(line_idx, lines):
    """هل يوجد with db_conn_lock محتط لهذا السطر (محاذاة أقل فوقه)؟"""
    ind = len(lines[line_idx]) - len(lines[line_idx].lstrip())
    for j in range(line_idx - 1, -1, -1):
        l = lines[j]
        if not l.strip() or l.strip().startswith('#'):
            continue
        ind_j = len(l) - len(l.lstrip())
        if ind_j < ind and 'with db_conn_lock' in l:
            return True
        if ind_j < ind and (l.strip().startswith('def ') or l.strip().startswith('class ')):
            return False
    return False

print('═══ 1) قفل اتصال DB المشترك ═══')
check('db_conn_lock موجود وهو RLock', isinstance(getattr(vnz, 'db_conn_lock', None), type(threading.RLock())))
bare_cursor = re.findall(r'with conn\.cursor\(\)', SRC)
check('لا مواقع كتابة/قراءة بقursor بلا قفل', len(bare_cursor) == 0, f'بقيت {len(bare_cursor)} موقعًا')
lock_sites = len(re.findall(r'with db_conn_lock', SRC))
check('قفل DB مستخدم في المواقع الحرجة (≥16)', lock_sites >= 16, f'استُخدم في {lock_sites} موقعًا')
# فحص دقيق بمحاذاة: كل conn.commit()/conn.rollback() مستقلة السطر يجب أن يكون داخل with db_conn_lock
_lines = SRC.split('\n')
_free = [i + 1 for i, l in enumerate(_lines)
         if re.match(r'^\s*conn\.(commit|rollback)\(\)', l) and not _enclosing_lock(i, _lines)]
check('لا conn.commit()/rollback() خارج قفل DB (ماسح محاذاة)', len(_free) == 0, f'sطور حرّة عند السطور: {_free[:8]}')

print('═══ 2) حارس الكتابة _cache_touch_open ═══')
with vnz.signal_cache_lock:
    vnz.open_signals_cache.clear()
    vnz.open_signals_cache['AAAUSDT'] = {'id': 7, 'symbol': 'AAAUSDT', 'status': 'open'}
sig = {'id': 7, 'symbol': 'AAAUSDT', 'status': 'open', 'stop_loss': 1.0}
check('كتابة مسموحة لنفس المعرف المفتوح', vnz._cache_touch_open('AAAUSDT', 7, sig) is True)
check('الكتابة نفذت فعلًا', vnz.open_signals_cache['AAAUSDT'].get('stop_loss') == 1.0)
# محاكاة الإغلاق: close_signal يحذف المفتاح ثم ت arrives كتابة قديمة
with vnz.signal_cache_lock:
    vnz.open_signals_cache.pop('AAAUSDT', None)
resurrected = vnz._cache_touch_open('AAAUSDT', 7, sig)
check('لا إحياء بعد الحذف (الإغلاق)', resurrected is False)
check('الكاش نظيف بلا إحياء', 'AAAUSDT' not in vnz.open_signals_cache)
with vnz.signal_cache_lock:
    vnz.open_signals_cache['AAAUSDT'] = {'id': 8, 'symbol': 'AAAUSDT', 'status': 'open'}
check('رفض كتابة بمعرف مختلف (صفقة أحدث)',
      vnz._cache_touch_open('AAAUSDT', 7, sig) is False and vnz.open_signals_cache['AAAUSDT']['id'] == 8)

print('═══ 3) المصالحة الثنائية _reconcile_cache_with_db ═══')
calls = {'load': []}
_orig_load = vnz.load_open_signals_to_cache
vnz.load_open_signals_to_cache = lambda retries=4, delay=10: calls['load'].append(list(vnz._reconcile_missing_holder) if hasattr(vnz, '_reconcile_missing_holder') else 'called')

class _FakeCur:
    def __init__(self, rows): self._rows = rows
    def execute(self, *a, **k): pass
    def fetchall(self): return self._rows
    def __enter__(self): return self
    def __exit__(self, *a): return False

class _FakeConn:
    def __init__(self, rows): self._rows = rows
    def cursor(self): return _FakeCur(self._rows)

# سيناريو: DB تفتح AAA(1) وBBB(2)؛ الكاش يحوي AAA(1) وCCC(3) (CCC مغلقة في DB)
with vnz.signal_cache_lock:
    vnz.open_signals_cache.clear()
    vnz.open_signals_cache['AAAUSDT'] = {'id': 1, 'symbol': 'AAAUSDT', 'status': 'open'}
    vnz.open_signals_cache['CCCUSDT'] = {'id': 3, 'symbol': 'CCCUSDT', 'status': 'open'}
vnz._reconcile_missing_holder = ['BBBUSDT']
_orig_cdc, _orig_conn = vnz.check_db_connection, vnz.conn
vnz.check_db_connection = lambda: True
vnz.conn = _FakeConn([{'id': 1, 'symbol': 'AAAUSDT', 'status': 'open'},
                      {'id': 2, 'symbol': 'BBBUSDT', 'status': 'open'}])
try:
    vnz._reconcile_cache_with_db()
finally:
    vnz.check_db_connection, vnz.conn = _orig_cdc, _orig_conn
    del vnz._reconcile_missing_holder
with vnz.signal_cache_lock:
    cache_now = dict(vnz.open_signals_cache)
check('الاتجاه 1: المغلقة في DB حُذفت من الكاش (CCC)', 'CCCUSDT' not in cache_now)
check('الاتجاه 1: المفتوحة المشتركة بقيت (AAA)', 'AAAUSDT' in cache_now)
check('الاتجاه 2: طُلب إعادة تحميل اليتيمة (BBB)', len(calls['load']) == 1)
vnz.load_open_signals_to_cache = _orig_load
# سيناريو اليتيمة الحقيقي: load الرسمي يعيد البناء من DB
with vnz.signal_cache_lock:
    vnz.open_signals_cache.clear()

print('═══ 4) إصلاح السقوط بعد الإغلاق في trade_management_loop ═══')
tm_src = SRC[SRC.index('def trade_management_loop():'):SRC.index('def collect_price_symbols()')]
jclose = tm_src.index("close_signal(signal_id, current_price, 'journey_completed')")
after_j = tm_src[jclose:jclose+400]
check('continue بعد journey_completed مباشرة', 'continue' in after_j[:200])
tpclose = tm_src.index("close_signal(signal_id, current_price, 'take_profit')")
after_t = tm_src[tpclose:tpclose+250]
check('continue بعد take_profit مباشرة', 'continue' in after_t[:120])
check('كتابة الذروة صارت مشروطة (_cache_touch_open)',
      tm_src.count('_cache_touch_open(symbol, signal_id, signal)') == 2)
check('لا كتابة حرة open_signals_cache[symbol] = signal في المدير',
      len(re.findall(r'open_signals_cache\[symbol\] = signal', tm_src)) == 0)
# مسارات الإغلاق الأخرى فيها continue أصلًا
for pat in ["close_signal(signal_id, current_price, reason)", "close_signal(signal_id, current_price, 'stale_time_exit')"]:
    i = tm_src.index(pat)
    check(f'continue موجود بعد {pat[33:60]}', 'continue' in tm_src[i:i+120])

print('═══ 5) تحقق ما بعد الإثبات في close_signal ═══')
cs_src = SRC[SRC.index('def close_signal('):SRC.index('def _symbol_recently_closed')]
check('SELECT تحقق بعد commit موجود', "SELECT status FROM signals WHERE id = %s" in cs_src)
check('إعادة تنفيذ عند فشل التحقق', 'إعادة التنفيذ' in cs_src)
check('الإغلاق والتحقق داخل قفل DB', cs_src.count('with db_conn_lock, conn.cursor()') >= 3)

print('═══ 6) نبضة البقاء الذاتية ═══')
check('RENDER_KEEP_ALIVE_SEC الافتراضي 540ث', vnz.RENDER_KEEP_ALIVE_SEC == 540)
check('لا URL محليًا → النبضة تعطل نفسها بأمان', vnz._KEEP_ALIVE_URL == '' and vnz.keep_alive_loop() is None)
check('النبضة تستهدف /health (معفاة من المصادقة)', '/health' in inspect.getsource(vnz.keep_alive_loop))
check('النبضة أول خيط عند الإقلاع (قبل initialize_bot_services)',
      SRC.index('Thread(target=keep_alive_loop') < SRC.index('Thread(target=initialize_bot_services'))
check('النبضة لا تعتمد على كرون خارجي', 'RENDER_EXTERNAL_URL' in SRC)

print('═══ 7) الإصدار ═══')
check('APP_VERSION = V9.30.0', vnz.APP_VERSION == 'V9.30.0')

print('════════════════════════════════')
print(f'النتيجة: {len(PASS)}/{len(PASS)+len(FAIL)} نجح')
if FAIL:
    print('فشل:', FAIL)
sys.exit(1 if FAIL else 0)
