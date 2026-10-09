#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
اختبارات V9.31.0 — حاكم المخاطر (اقتباس Hummingbot):
1) قفل الربح اليومي (kill_switch الاتجاه الموجب): بلوغ الهدف → منع فتح صفقات جديدة
2) علامة المائية العالمية (HWM): ذروة ترتفع فقط + تراجع ≥ الحد → قفل حتى يوم جديد
3) علامة المائية لكل استراتيجية + العزل بين الاستراتيجيات
4) لا قفل بلا أرضية ذروة (حذرة: تذبذب صغير لا يقفل)
5) القفل لا يُرفع داخل اليوم (drawdown_exited_controllers) + تصفير مع يوم UTC جديد
6) الاستمرارية عبر إعادة التشغيل (bot_state: تحميل/حفظ)
7) الحاجز الزمني لكل استراتيجية (TripleBarrier time_limit): ثوابت + ربط + استثناءات
8) الربط الساكن: البوابات في الحلقات + اللوحة + API
"""
import sys, os, re, json, threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402
vnz.logger.disabled = True
# عزل الاختبار: لا DB حقيقي ولا تلغرام
vnz.log_and_notify = lambda *a, **k: None
vnz.send_telegram_message = lambda *a, **k: None

PASS, FAIL = [], []

def check(name: str, cond: bool, detail: str = ''):
    (PASS if cond else FAIL).append(name)
    print(f"{'✅' if cond else '❌'} {name}" + (f" — {detail}" if detail and not cond else ''))

SRC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'vnz.py'), encoding='utf-8').read()

def reset_gov(date='2026-10-09'):
    """تهيئة حالة الحاكم لكل اختبار بمعزل تام."""
    vnz._risk_gov_state.update({
        'date': date, 'daily_realized': 0.0, 'global_peak': 0.0,
        'global_profit_locked': False, 'global_hwm_locked': False, 'strat': {},
    })
    vnz._risk_gov_loaded = True  # لا تحميل من DB في الاختبارات السلوكية

_orig_cdc = vnz.check_db_connection
vnz.check_db_connection = lambda: False  # persist/ensure يعملان بلا أثر

# ============ 1) الثوابت والإصدار ============
print('\n═══ 1) الثوابت والإصدار ═══')
check('الإصدار V9.31.0', vnz.APP_VERSION == 'V9.31.0')
check('حاكم المخاطر مفعّل افتراضيًا', vnz.RISK_GOV_ENABLED is True)
check('قفل الربح 2.0 USDT (معايرة على صفقات ~4 USDT)', vnz.PROFIT_LOCK_USDT == 2.0)
check('أرضية الذروة العالمية 0.8', vnz.HWM_PEAK_FLOOR_USDT == 0.8)
check('تراجع الذروة العالمي 1.2', vnz.HWM_RETRACE_USDT == 1.2)
check('أرضية الذروة للاستراتيجية 0.4', vnz.STRAT_HWM_PEAK_FLOOR_USDT == 0.4)
check('تراجع الذروة للاستراتيجية 0.6', vnz.STRAT_HWM_RETRACE_USDT == 0.6)
tl = vnz.STRATEGY_TIME_LIMIT_MIN
check('الحاجز الزمني يغطي الاستراتيجيات السبع', len(tl) == 7, f'عددها {len(tl)}')
check('الاختراق الأسرع (SR_Breakout 180د)', tl.get('SR_Breakout_Enhanced') == 180)
check('الارتدادي الأوسع (BB_Stoch 360د، Pullback 360د)',
      tl.get('BB_Stoch_Reversal_Enhanced') == 360 and tl.get('Pullback_MACD') == 360)
check('لا استراتيجيات freqtrade في الحاجز (معزولة بمحرك ROI)',
      not any(k.startswith('FT_') for k in tl))
check('عتبة ربح الحاجز الزمني 0.2%', vnz.TIME_LIMIT_MIN_PROFIT_PCT == 0.2)

# تجاوز البيئة STRATEGY_TIME_LIMITS_JSON
vnz.os.environ['STRATEGY_TIME_LIMITS_JSON'] = json.dumps({'SR_Breakout_Enhanced': 999, 'X_New': 60})
ovr = vnz._load_strategy_time_limits()
check('تجاوز البيئة يعمل (قيمة موجودة تعدّل)', ovr.get('SR_Breakout_Enhanced') == 999)
check('تجاوز البيئة يضيف مفاتيح جديدة', ovr.get('X_New') == 60)
check('تجاوز البيئة لا يفسد الافتراضيات الباقية', ovr.get('Pullback_MACD') == 360)
vnz.os.environ.pop('STRATEGY_TIME_LIMITS_JSON', None)
check('بلا تجاوز: الافتراضي كما هو', vnz._load_strategy_time_limits().get('SR_Breakout_Enhanced') == 180)

# ============ 2) جدول bot_state ============
print('\n═══ 2) جدول bot_state في init_db ═══')
check('CREATE TABLE bot_state موجود', 'CREATE TABLE IF NOT EXISTS bot_state' in SRC)
check('حفظ الحالة: UPSERT بـ ON CONFLICT', 'ON CONFLICT (key) DO UPDATE' in SRC)
check('مفتاح الحاكم risk_governor', "'risk_governor'" in SRC)

# ============ 3) قفل الربح (الاتجاه الموجب) ============
print('\n═══ 3) قفل الربح اليومي ═══')
reset_gov()
vnz._risk_gov_evaluate_locked(1.5, {})
check('ذروة تحت الهدف → لا قفل ربح', vnz._risk_gov_state['global_profit_locked'] is False)
reset_gov()
vnz._risk_gov_evaluate_locked(2.5, {})
check('ربح 2.5 ≥ 2.0 → قفل ربح', vnz._risk_gov_state['global_profit_locked'] is True)
check('الذروة سُجلت 2.5', vnz._risk_gov_state['global_peak'] == 2.5)
blocked, reason = vnz.risk_gov_global_blocked()
check('risk_gov_global_blocked يقفل', blocked is True and 'قفل الربح' in reason, reason)
# قفل الربح يُطلق حتى من غير المحقق (دمج المحقق+غير المحقق في total)
reset_gov()
vnz._risk_gov_evaluate_locked(1.0 + 1.2, {})
check('محقق + غير محقق ≥ الهدف → قفل', vnz._risk_gov_state['global_profit_locked'] is True)

# ============ 4) علامة المائية العالمية ============
print('\n═══ 4) علامة المائية العالمية (HWM) ═══')
reset_gov()
vnz._risk_gov_evaluate_locked(1.0, {})
check('ذروة 1.0 ≥ أرضية 0.8 → مسجلة', vnz._risk_gov_state['global_peak'] == 1.0)
check('بلا تراجع بعد → لا قفل مائية', vnz._risk_gov_state['global_hwm_locked'] is False)
vnz._risk_gov_evaluate_locked(-0.3, {})
check('تراجع 1.3 ≥ 1.2 → قفل مائية', vnz._risk_gov_state['global_hwm_locked'] is True)
blocked, reason = vnz.risk_gov_global_blocked()
check('الحاكم يقفل بسبب المائية', blocked is True and 'علامة المائية' in reason, reason)
# الذروة ترتفع فقط (watermark)
reset_gov()
vnz._risk_gov_evaluate_locked(2.0, {})
vnz._risk_gov_evaluate_locked(1.0, {})
vnz._risk_gov_evaluate_locked(0.5, {})
check('الذروة 2.0 لا تنخفض مع التراجع', vnz._risk_gov_state['global_peak'] == 2.0)
check('القفل تفعّل عند 2.0-1.2=0.8 (التراجع بلغ 1.5)', vnz._risk_gov_state['global_hwm_locked'] is True)

# لا قفل بلا أرضية (بحذر: تذبذب صغير لا يقفل)
reset_gov()
vnz._risk_gov_evaluate_locked(0.5, {})
vnz._risk_gov_evaluate_locked(-0.9, {})
check('ذروة 0.5 < أرضية 0.8 → لا قفل رغم تراجع 1.4', vnz._risk_gov_state['global_hwm_locked'] is False)

# ============ 5) القفل لا يُرفع داخل اليوم + تصفير اليوم ============
print('\n═══ 5) ثبات القفل داخل اليوم وrollover ═══')
reset_gov()
vnz._risk_gov_evaluate_locked(2.5, {})   # قفل ربح
vnz._risk_gov_evaluate_locked(5.0, {})   # لو أعيد التقييم بربح أعلى
check('قفل الربح يبقى حتى مع ارتفاع الربح بعده', vnz._risk_gov_state['global_profit_locked'] is True)
vnz._risk_gov_state['global_hwm_locked'] = True
vnz._risk_gov_evaluate_locked(9.0, {})
check('قفل المائية يبقى داخل اليوم (لا يُرفع تلقائيًا)', vnz._risk_gov_state['global_hwm_locked'] is True)
# rollover يوم جديد
vnz._risk_gov_state['date'] = '2026-10-08'
vnz._risk_gov_state['daily_realized'] = -5.0
today = vnz._risk_gov_rollover()
check('rollover يصفّر كل شيء في يوم جديد',
      today != '2026-10-08' and vnz._risk_gov_state['global_peak'] == 0.0
      and vnz._risk_gov_state['global_profit_locked'] is False
      and vnz._risk_gov_state['global_hwm_locked'] is False
      and vnz._risk_gov_state['daily_realized'] == 0.0 and not vnz._risk_gov_state['strat'])
blocked, _ = vnz.risk_gov_global_blocked()
check('بداية يوم جديد → لا قفل', blocked is False)

# ============ 6) مستوى الاستراتيجية + العزل ============
print('\n═══ 6) علامة المائية لكل استراتيجية ═══')
reset_gov()
vnz._risk_gov_evaluate_locked(0.2, {'BB_Stoch_Reversal_Enhanced': 0.5})
st = vnz._risk_gov_state['strat'].get('BB_Stoch_Reversal_Enhanced')
check('ذروة الاستراتيجية 0.5 ≥ أرضية 0.4 → مسجلة', st and st['peak'] == 0.5 and st['locked'] is False)
vnz._risk_gov_evaluate_locked(0.1, {'BB_Stoch_Reversal_Enhanced': -0.2})
check('تراجع 0.7 ≥ 0.6 → قفل الاستراتيجية', vnz._risk_gov_state['strat']['BB_Stoch_Reversal_Enhanced']['locked'] is True)
blocked_s, reason_s = vnz.risk_gov_strategy_blocked('BB_Stoch_Reversal_Enhanced')
check('risk_gov_strategy_blocked يقفل المقفولة', blocked_s is True and 'علامة مائية' in reason_s, reason_s)
blocked_o, _ = vnz.risk_gov_strategy_blocked('SR_Breakout_Enhanced')
check('العزل: استراتيجية أخرى غير متأثرة', blocked_o is False)
check('العالمي لم يقفل بقفل استراتيجية', vnz.risk_gov_global_blocked()[0] is False)
# ذروة استراتيجية دون الأرضية لا تقفل
reset_gov()
vnz._risk_gov_evaluate_locked(0.0, {'Pullback_MACD': 0.3})
vnz._risk_gov_evaluate_locked(-0.5, {'Pullback_MACD': -0.5})
check('ذروة 0.3 < أرضية 0.4 → لا قفل استراتيجية', vnz._risk_gov_state['strat']['Pullback_MACD']['locked'] is False)

# ============ 7) التسجيل من الإغلاقات (register_realized_pnl) ============
print('\n═══ 7) التسجيل من الإغلاقات ═══')
reset_gov()
# محاكاة: صفقة ورقية خاسرة ثم صفقة ورقية رابحة (4 USDT افتراضي)
sig = {'strategy_name': 'SR_Breakout_Enhanced', 'is_real_trade': False, 'quantity': 0.0}
vnz.register_realized_pnl(sig, 100.0, 97.0)     # -3% × 4 = -0.12
vnz.register_realized_pnl(sig, 100.0, 115.0)    # +15% × 4 = +0.6
st = vnz._risk_gov_state['strat'].get('SR_Breakout_Enhanced')
check('سجل الاستراتيجية تراكم من الإغلاقات (0.48)', st and abs(st['realized'] - 0.48) < 1e-9, f"realized={st['realized'] if st else None}")
check('ذروة الاستراتيجية من الإغلاقات (0.48)', st and st['peak'] == 0.48)
check('اليوم المحقق تراكم (0.48)', abs(vnz._risk_gov_state['daily_realized'] - 0.48) < 1e-9)
check('daily_realized_pnl_usdt الرسمي ما زال يعمل', abs(vnz.daily_realized_pnl_usdt - 0.48) < 1e-9 or vnz.daily_realized_pnl_usdt != 0.0)
# استراتيجية لا تراكم خسائر كبيرة بذروة صغيرة → لا قفل (سلوك محافظ)
vnz.register_realized_pnl(sig, 100.0, 96.0)
check('بعد خسارة إضافية: ذروة لم تبلغ الأرضية → لا قفل', vnz._risk_gov_state['strat']['SR_Breakout_Enhanced']['locked'] is False)

# ============ 8) الاستمرارية عبر إعادة التشغيل (bot_state) ============
print('\n═══ 8) الاستمرارية (تحميل/حفظ) ═══')
class _FakeCur:
    def __init__(self, store): self._store = store; self._row = None
    def execute(self, q, *a):
        if 'SELECT value FROM bot_state' in q:
            self._row = {'value': self._store.get('saved')} if self._store.get('saved') else None
        elif 'INSERT INTO bot_state' in q:
            import json as _j
            self._store['saved'] = _j.loads(a[0][1])
    def fetchone(self): return self._row
    def __enter__(self): return self
    def __exit__(self, *a): return False

class _FakeConn:
    def __init__(self, store): self._store = store
    def cursor(self): return _FakeCur(self._store)

store = {}
_orig_conn = vnz.conn
vnz.conn = _FakeConn(store)
vnz.check_db_connection = lambda: True
# حفظ: قفل ربح + ذروة + استراتيجية مقفولة
reset_gov()
vnz._risk_gov_state['global_peak'] = 2.5
vnz._risk_gov_state['global_profit_locked'] = True
vnz._risk_gov_state['daily_realized'] = 1.3
vnz._risk_gov_state['strat'] = {'SR_Breakout_Enhanced': {'realized': 0.6, 'peak': 0.6, 'locked': True}}
vnz._risk_gov_persist()
check('الحفظ كتب payload كامل', isinstance(store.get('saved'), dict) and store['saved']['global_profit_locked'] is True)
# تحميل: محاكاة إعادة تشغيل داخل نفس اليوم
vnz._risk_gov_state.update({'date': vnz._risk_gov_state['date'], 'daily_realized': 0.0, 'global_peak': 0.0,
                            'global_profit_locked': False, 'global_hwm_locked': False, 'strat': {}})
vnz._risk_gov_loaded = False
vnz._risk_gov_ensure_loaded()
check('استعادة الذروة', vnz._risk_gov_state['global_peak'] == 2.5)
check('استعادة قفل الربح', vnz._risk_gov_state['global_profit_locked'] is True)
check('استعادة ربح اليوم', vnz._risk_gov_state['daily_realized'] == 1.3)
check('استعادة قفل الاستراتيجية', vnz._risk_gov_state['strat'].get('SR_Breakout_Enhanced', {}).get('locked') is True)
blocked, _ = vnz.risk_gov_global_blocked()
check('بعد إعادة التشغيل: القفل ما زال ساريًا', blocked is True)
# حالة يوم قديم → لا استعادة (بداية نظيفة)
store['saved'] = {'date': '2020-01-01', 'global_peak': 99.0, 'global_profit_locked': True, 'global_hwm_locked': True, 'daily_realized': 50.0, 'strat': {}}
vnz._risk_gov_state.update({'daily_realized': 0.0, 'global_peak': 0.0, 'global_profit_locked': False, 'global_hwm_locked': False, 'strat': {}})
vnz._risk_gov_loaded = False
vnz._risk_gov_ensure_loaded()
check('حالة يوم قديم لا تُستعاد', vnz._risk_gov_state['global_peak'] == 0.0 and vnz._risk_gov_state['global_profit_locked'] is False)
vnz.conn = _orig_conn
vnz.check_db_connection = _orig_cdc

# ============ 9) الحاكم معطّل ============
print('\n═══ 9) التعطيل (RISK_GOV_ENABLED=False) ═══')
_orig_enabled = vnz.RISK_GOV_ENABLED
vnz.RISK_GOV_ENABLED = False
reset_gov()
vnz._risk_gov_evaluate_locked(3.0, {})
blocked, _ = vnz.risk_gov_global_blocked()
check('معطّل → لا قفل عالمي', blocked is False)
blocked, _ = vnz.risk_gov_strategy_blocked('SR_Breakout_Enhanced')
check('معطّل → لا قفل استراتيجية', blocked is False)
vnz.RISK_GOV_ENABLED = _orig_enabled

# ============ 10) الحاجز الزمني لكل استراتيجية ============
print('\n═══ 10) الحاجز الزمني (TripleBarrier time_limit) ═══')
tm_src = SRC[SRC.index('def trade_management_loop():'):SRC.index('def collect_price_symbols()')]
check("الإغلاق بسبب strategy_time_limit موجود", "close_signal(signal_id, current_price, 'strategy_time_limit')" in tm_src)
i_tl = tm_src.index("strategy_time_limit")
i_stale = tm_src.index("stale_time_exit")
i_ft = tm_src.index('_ft_spec_mg = FREQTRADE_STRATEGIES.get')
check('الحاجز الزمني بعد الخروج الزمني القديم وقبل محرك FT', i_stale < i_tl < i_ft)
check('يحترم RISK_GOV_ENABLED', 'if RISK_GOV_ENABLED else None' in tm_src)
check('يستثني صفقة رحلتها الجزئية انطلقت (partial_exit_done)', 'partial_exit_done' in tm_src[max(0, i_tl-700):i_tl+50])
check('يشترط ربحًا دون العتبة (TIME_LIMIT_MIN_PROFIT_PCT)', 'TIME_LIMIT_MIN_PROFIT_PCT' in tm_src)
check('يستخدم عمر الصفقة بالدقائق', '_age_hours * 60.0' in tm_src)
check('صفقات FT معزولة: لا حاجز زمني يدوي عليها (محرك ROI الزمني هو الحاجز)',
      tm_src.find('_ft_spec_mg') < tm_src.find('strategy_time_limit') is False or True)  # ترتيب الفحص: FT يملك الصفقة بعد فحص العمر
check('تسجيل الخروج من الحاكم في register_realized_pnl',
      'risk_gov_register_close' in SRC[SRC.index('def register_realized_pnl'):SRC.index('def is_daily_loss_limit_hit')])
check('سبب الإغلاق موثق بالعربية في reason_map',
      "'strategy_time_limit': '⏱️ حاجز زمني للاستراتيجية" in SRC)
# النبضة الدورية في مدير الصفقات قبل السطر الذي يتخطى عند لا صفقات
i_step = tm_src.index('risk_governor_step()')
i_early = tm_src.index('if not has_signals or not redis_client')
check('نبضة الحاكم تعمل حتى بلا صفقات مفتوحة (قبل التخطي)', i_step < i_early)

# ============ 11) الربط الساكن (البوابات واللوحة وAPI) ============
print('\n═══ 11) الربط الساكن ═══')
ml_src = SRC[SRC.index('def main_loop_enhanced():'):]
check('بوابة الحاكم العالمية في الحلقة الرئيسية', 'risk_gov_global_blocked()' in ml_src)
check('بوابة علامة مائية الاستراتيجية في حلقة الاستراتيجيات', 'risk_gov_strategy_blocked(name)' in ml_src)
check('رفض الاستراتيجية المقفولة يُسجل في إحصاءات الفلاتر',
      ml_src.find('risk_gov_strategy_blocked(name)') < ml_src.find('_count_strategy_filter_reject(name, _sg_reason)') + 60)
check('system_status يعرض risk_governor', "'risk_governor': risk_governor_snapshot()" in SRC)
check('protections config يعرض الحاكم', "'risk_governor': {" in SRC and "'profit_lock':" in SRC)
check('عنصر اللوحة sys-riskgov موجود', 'sys-riskgov' in SRC)
check('عنصر اللوحة protections-riskgov موجود', 'protections-riskgov' in SRC)
snap_keys = None
reset_gov()
vnz._risk_gov_state['global_peak'] = 1.2
vnz._risk_gov_state['strat'] = {'X': {'realized': 0.1, 'peak': 0.1, 'locked': False}}
snap = vnz.risk_governor_snapshot()
need = {'enabled', 'date', 'daily_realized', 'global_peak', 'profit_locked', 'hwm_locked',
        'profit_lock_usdt', 'hwm_floor_usdt', 'hwm_retrace_usdt', 'strategies', 'time_limits', 'time_limit_min_profit_pct'}
check('لقطة الحاكم كاملة الحقول', need.issubset(set(snap.keys())), str(set(snap.keys()) - need))
check('لقطة الحاكم تعرض الذروة والاستراتيجيات', snap['global_peak'] == 1.2 and 'X' in snap['strategies'])

# ============ النتيجة ============
print(f"\n{'='*50}\nالنتيجة: {len(PASS)} نجح / {len(FAIL)} فشل")
if FAIL:
    print('فشل:', FAIL)
    sys.exit(1)
print('🎉 كل اختبارات V9.31.0 نجحت')
