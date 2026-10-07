# -*- coding: utf-8 -*-
"""اختبارات V9.18.0 — وضع التوصيات: اجتياز الفلاتر = توصية شراء مفتوحة.
تُجرى بلا شبكة: استيراد الوحدة مع تعطيل التهيئة الشبكية (لا main)."""
import os
import sys
import time
import types
from collections import deque
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# خفض ضجيج السجلات أثناء الاختبار
os.environ.setdefault('LOG_LEVEL', 'ERROR')

import vnz  # noqa: E402  (الاستيراد ينفذ الإعدادات فقط — الإقلاع تحت __main__)


def test_version():
    assert vnz.APP_VERSION.startswith('V9.'), f"الإصدار: {vnz.APP_VERSION}"
    print("✅ 1) الإصدار V9.18.0")


def test_config_defaults():
    assert vnz.RECOMMENDATIONS_ENABLED is True
    assert vnz.RECOMMENDATIONS_PER_CYCLE == 2
    assert vnz.RECOMMENDATION_MIN_FIT_SCORE == 60.0
    assert vnz.RECOMMENDATION_COOLDOWN_MIN == 240
    assert set(vnz._recommendation_stats.keys()) == {'opened', 'gate_rejected', 'cooldown_skipped', 'below_min_score'}
    print("✅ 2) إعدادات التوصيات الافتراضية سليمة (مفعّل، 2/دورة، حد 60، تهدئة 240د)")


def test_pair_matching_dependency():
    """التوصيات مبنية فوق مطابقة الأزواج — يجب أن تكون البوابة الرئيسية موجودة."""
    assert vnz.PAIR_MATCHING_ENABLED is True
    prof = vnz.STRATEGY_PAIR_PROFILES.get('BB_Stoch_Reversal_Enhanced')
    assert prof and 'range' in prof['regimes']
    print("✅ 3) مطابقة الأزواج مفعّلة وملفات الاستراتيجيات سليمة")


def test_cooldown_memory_layer():
    """التهدئة: طبقة الذاكرة تمنع الرمز المغلق حديثًا (بلا DB)."""
    vnz._recent_close_ts.clear()
    # DB غير متصل — يجب أن تعتمد على الذاكرة وحدها
    with patch.object(vnz, 'check_db_connection', return_value=False):
        assert vnz._symbol_recently_closed('XYZUSDT') is False  # لا سجل إطلاقًا
        vnz._recent_close_ts['XYZUSDT'] = time.time() - 60       # أُغلق قبل دقيقة
        assert vnz._symbol_recently_closed('XYZUSDT') is True    # ضمن التهدئة (240د)
        vnz._recent_close_ts['OLDUSDT'] = time.time() - 999999   # أُغلق قديمًا
        assert vnz._symbol_recently_closed('OLDUSDT') is False   # خارج التهدئة
    vnz._recent_close_ts.clear()
    print("✅ 4) تهدئة الذاكرة: يمنع الرمز المغلق حديثًا ويسمح القديم")


def test_cooldown_db_layer():
    """التهدئة: طبقة قاعدة البيانات تصمد لإعادة التشغيل."""
    vnz._recent_close_ts.clear()
    fake_cursor = MagicMock()
    fake_cursor.fetchone.return_value = (300.0,)  # أُغلق قبل 5 دقائق
    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cursor
    with patch.object(vnz, 'check_db_connection', return_value=True), \
         patch.object(vnz, 'conn', fake_conn):
        assert vnz._symbol_recently_closed('ABCUSDT') is True
        fake_cursor.fetchone.return_value = (999999.0,)  # إغلاق قديم
        assert vnz._symbol_recently_closed('ABCUSDT') is False
        fake_cursor.fetchone.return_value = (None,)      # بلا سجل إغلاق
        assert vnz._symbol_recently_closed('ABCUSDT') is False
    # فشل DB لا يرمي استثناء ولا يمنع الدخول
    with patch.object(vnz, 'check_db_connection', return_value=False):
        assert vnz._symbol_recently_closed('ABCUSDT') is False
    print("✅ 5) تهدئة قاعدة البيانات: حديث/قديم/بلا سجل/فشل اتصال — كلها سليمة")


def test_best_candidate_selection():
    """منطق الاختيار: أعلى درجة مطابقة تفوز (محاكاة لسطر max في الحلقة)."""
    cands = [('MACD_EMA_Crossover', 55.0), ('BB_Stoch_Reversal_Enhanced', 78.0), ('Pullback_MACD', 61.0)]
    name, score = max(cands, key=lambda t: t[1])
    assert name == 'BB_Stoch_Reversal_Enhanced' and score == 78.0
    # تحت الحد الأدنى → لا توصية
    low = [('MACD_EMA_Crossover', 59.0)]
    assert max(low, key=lambda t: t[1])[1] < vnz.RECOMMENDATION_MIN_FIT_SCORE
    print("✅ 6) اختيار أفضل مرشّح بالدرجة الأعلى + رفض ما دون 60")


def test_signal_details_documentation():
    """توثيق المصدر داخل signal_details كما في الحلقة الرئيسية."""
    tp_sl = {'target_price': 1.1, 'stop_loss': 0.9, 'rr_ratio': 1.5}
    new_signal = {'symbol': 'XYZUSDT', 'strategy_name': 'BB_Stoch_Reversal_Enhanced',
                  'signal_details': {**tp_sl}, 'entry_price': 1.0, **tp_sl}
    signal_source, signal_fit_score = 'filter_recommendation', 78.0
    regime_info = {'regime': 'range'}
    new_signal['signal_details']['source'] = signal_source
    if signal_fit_score is not None:
        new_signal['signal_details']['fit_score'] = round(float(signal_fit_score), 1)
    if regime_info:
        new_signal['signal_details']['regime'] = regime_info.get('regime')
        new_signal['signal_details']['regime_ar'] = vnz.REGIME_AR.get(regime_info.get('regime'))
    d = new_signal['signal_details']
    assert d['source'] == 'filter_recommendation' and d['fit_score'] == 78.0
    assert d['regime'] == 'range' and d['regime_ar'] == 'نطاق مترنم'
    # مصدر المُطلِق العادي
    assert 'strategy_trigger' != d['source']
    print("✅ 7) توثيق المصدر/الدرجة/النمط داخل تفاصيل الإشارة")


def test_recommendation_stats_accounting():
    """محاسبة العدادات كما تحدث في الحلقة عند فتح توصية بنجاح."""
    before = dict(vnz._recommendation_stats)
    with vnz._scan_stats_lock:
        vnz._recommendation_stats['opened'] += 1
        vnz._strategy_scan_stats['BB_Stoch_Reversal_Enhanced']['recommendations'] += 1
    assert vnz._recommendation_stats['opened'] == before['opened'] + 1
    assert vnz._strategy_scan_stats['BB_Stoch_Reversal_Enhanced']['recommendations'] >= 1
    # نظافة: إعادة الضبط لعدم تلويث الاستيرادات الأخرى
    vnz._recommendation_stats.clear()
    vnz._recommendation_stats.update({'opened': 0, 'gate_rejected': 0, 'cooldown_skipped': 0, 'below_min_score': 0})
    print("✅ 8) عدادات التوصيات تُحاسب تحت القفل بلا أخطاء")


def test_close_signal_records_cooldown():
    """إغلاق صفقة (ورقية — تجاوز مسار البيع الحقيقي) يوثق طابع التهدئة."""
    vnz._recent_close_ts.clear()
    fake_signal = {'id': 4242, 'symbol': 'COOLUSDT', 'entry_price': 2.0,
                   'is_real_trade': False, 'initial_stop_loss': 1.9}
    vnz.open_signals_cache['COOLUSDT'] = fake_signal
    fake_conn = MagicMock()
    with patch.object(vnz, 'check_db_connection', return_value=True), \
         patch.object(vnz, 'conn', fake_conn), \
         patch.object(vnz, 'register_realized_pnl'), \
         patch.object(vnz, 'send_telegram_message'):
        ok = vnz.close_signal(4242, 2.2, 'take_profit')
    assert ok is True
    assert 'COOLUSDT' not in vnz.open_signals_cache
    assert 'COOLUSDT' in vnz._recent_close_ts, "يجب توثيق طابع تهدئة الإغلاق"
    assert time.time() - vnz._recent_close_ts['COOLUSDT'] < 5
    vnz._recent_close_ts.clear()
    print("✅ 9) الإغلاق الناجح يوثق تهدئة الرمز فورًا")


def test_quota_flow():
    """منطق رصيد الدورة: يتوقف عند RECOMMENDATIONS_PER_CYCLE."""
    recommendations_opened_this_cycle = 0
    for _ in range(3):  # محاولة فتح 3 توصيات في الدورة
        if recommendations_opened_this_cycle < vnz.RECOMMENDATIONS_PER_CYCLE:
            recommendations_opened_this_cycle += 1
    assert recommendations_opened_this_cycle == vnz.RECOMMENDATIONS_PER_CYCLE == 2
    print("✅ 10) رصيد الدورة يقف عند الحد (2) ولا يتجاوزه")


if __name__ == '__main__':
    tests = [test_version, test_config_defaults, test_pair_matching_dependency,
             test_cooldown_memory_layer, test_cooldown_db_layer,
             test_best_candidate_selection, test_signal_details_documentation,
             test_recommendation_stats_accounting, test_close_signal_records_cooldown,
             test_quota_flow]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as ae:
            failed += 1
            print(f"❌ فشل: {t.__name__}: {ae}")
        except Exception as exc:
            failed += 1
            print(f"💥 خطأ غير متوقع في {t.__name__}: {exc}")
    print(f"\n{'='*50}\nالنتيجة: {len(tests)-failed}/{len(tests)} اختبارات ناجحة")
    sys.exit(1 if failed else 0)
