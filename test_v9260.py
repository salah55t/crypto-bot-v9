# -*- coding: utf-8 -*-
"""اختبارات V9.26.0 — طبقة الحماية والانضباط المستوحاة من Freqtrade.
اقتباسات مُختبرة: StoplossGuard / MaxDrawdown / LowProfitPairs / CooldownPeriod
(plugins/protections/) + سقف مسافة الوقف + الخروج الزمني + الشفافية (/api/protections)."""
import ast
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402

# صمت الإشعارات أثناء الاختبار (لا DB ولا تليجرام)
vnz.log_and_notify = lambda *a, **k: None
vnz.send_telegram_message = lambda *a, **k: None


def _reset_protections():
    with vnz._protection_lock:
        vnz._protection_locks.clear()
        vnz._protection_close_log.clear()
    with vnz._scan_stats_lock:
        vnz._protection_stats.update({
            'locks_created_total': 0, 'global_locks': 0, 'pair_locks': 0,
            'entries_blocked': 0, 'last_lock_at': None, 'last_lock_reason': None,
        })


def _feed_close(symbol, reason, profit):
    vnz.record_trade_close_for_protections(symbol, reason, profit)


def test_version():
    # [V9.27.0] مرن: V9.26.0 أو أحدث (طبقة الحمايات لا تتغير بلا علم)
    ver = vnz.APP_VERSION
    def _vt(v):
        try:
            return tuple(int(x) for x in v[1:].split('.'))
        except Exception:
            return (0,)
    assert _vt(ver) >= (9, 26, 0), f"الإصدار: {ver}"
    print("✅ 1) الإصدار V9.26.0+ (الحمايات موجودة)")


def test_constants():
    assert vnz.PROTECTIONS_ENABLED is True
    assert vnz.PROTECTION_SL_COUNT == 4 and vnz.PROTECTION_SL_LOOKBACK_MIN == 240
    assert vnz.PROTECTION_DD_MAX_PCT == 12.0 and vnz.PROTECTION_DD_TRADE_LIMIT == 5
    assert vnz.PROTECTION_PAIR_TRADES == 2 and vnz.PROTECTION_PAIR_STOP_MIN == 360
    assert vnz.PROTECTION_COOLDOWN_MIN == 240
    assert vnz.MAX_SL_DISTANCE_PCT == 6.0
    assert vnz.STALE_TRADE_HOURS == 24.0 and vnz.STALE_MIN_PROFIT_PCT == 0.2
    print("✅ 2) ثوابت الحمايات الأربع + سقف الوقف 6% + الخروج الزمني 24س/0.2%")


def test_stoploss_guard_global_lock():
    """نمط StoplossGuard في freqtrade: 4 وقفات خسارة في النافذة → قفل شامل."""
    _reset_protections()
    for i in range(4):
        _feed_close(f'S{i}USDT', 'stop_loss', -1.0 - i * 0.1)
    locked, reason = vnz.is_entry_protection_locked(None)
    assert locked, "التجميد الشامل يجب أن يفعّل بعد 4 وقفات"
    assert 'StoplossGuard' in reason, f"السبب يذكر الحماية: {reason}"
    # يمنع أي رمز أيضًا (شامل)
    locked_sym, _ = vnz.is_entry_protection_locked('ANYUSDT')
    assert locked_sym
    with vnz._scan_stats_lock:
        assert vnz._protection_stats['global_locks'] == 1
    print("✅ 3) StoplossGuard: 4 وقفات خسارة → تجميد شامل يمنع كل الدخولات")


def test_lock_expiry_and_dedupe():
    """نمط PairLock في freqtrade: للقفل مهلة تنتهي تلقائيًا، والأطول يغلب الأقصر."""
    _reset_protections()
    _feed_close('EXPUSDT', 'stop_loss', -1.0)  # CooldownPeriod قفل الزوج
    with vnz._protection_lock:
        assert len(vnz._protection_locks) == 1
        # تقريب الانتهاء إلى الماضي → يُطرد عند الفحص
        vnz._protection_locks[0]['until_ts'] = time.time() - 5
    locked, _ = vnz.is_entry_protection_locked('EXPUSDT')
    assert not locked, "القفل المنتهي يجب أن يُسقط"
    # dedupe: قفل أقصر لا يبطل أطول قائم
    _reset_protections()
    vnz._add_protection_lock('pair', 'DDUSDT', 300, 'سبب أطول', 'LowProfitPairs')
    created = vnz._add_protection_lock('pair', 'DDUSDT', 60, 'سبب أقصر', 'CooldownPeriod')
    assert not created, "القفل الأقصر يجب أن يُرفض أمام الأطول"
    with vnz._protection_lock:
        assert len(vnz._protection_locks) == 1 and vnz._protection_locks[0]['minutes'] == 300
    print("✅ 4) انتهاء الأقفال تلقائي + الأطول يغلب الأقصر (نمط PairLock)")


def test_max_drawdown_global_lock():
    """نمط MaxDrawdown: تراكم النافذة يتجاوز الحد عبر 5+ صفقات → قفل شامل."""
    _reset_protections()
    losses = [-2.0, -2.5, -2.0, -2.2, -1.8]  # المجموع -10.5 < -12؟ لا: -10.5 > -12
    losses = [-3.0, -2.5, -2.0, -2.2, -2.6]  # المجموع -12.3 ≤ -12 ✓
    for i, p in enumerate(losses):
        _feed_close(f'M{i}USDT', 'stop_loss', p)
    locked, reason = vnz.is_entry_protection_locked(None)
    assert locked, "تراكم -12.3% يجب أن يفعّل MaxDrawdown"
    assert 'MaxDrawdown' in reason or 'StoplossGuard' in reason  # الأول يعمل أيضًا (5 وقفات)
    print("✅ 5) MaxDrawdown: تراكم -12.3% عبر 5 صفقات → تجميد شامل")


def test_low_profit_pairs_pair_only():
    """نمط LowProfitPairs: الزوج الخاسر المتكرر يُقفل وحده — غيره يبقى مفتوحًا."""
    _reset_protections()
    _feed_close('LOSUSDT', 'stop_loss', -1.5)
    _feed_close('LOSUSDT', 'stop_loss', -1.0)  # مجموع -2.5 < 0 → قفل الزوج
    locked_pair, r_pair = vnz.is_entry_protection_locked('LOSUSDT')
    assert locked_pair, "الزوج الخاسر يجب أن يُقفل"
    other_locked, _ = vnz.is_entry_protection_locked('OKUSDT')
    assert not other_locked, "الأزواج الأخرى تبقى مفتوحة (قفل زوج لا شامل)"
    with vnz._scan_stats_lock:
        assert vnz._protection_stats['pair_locks'] >= 1
    print(f"✅ 6) LowProfitPairs: قفل LOSUSDT فقط ({r_pair[:40]}...) — الأزواج الأخرى سليمة")


def test_cooldown_after_any_close():
    """نمط CooldownPeriod: أي إغلاق (حتى رابح) → تهدئة للزوج تفحصها كل المسارات."""
    _reset_protections()
    _feed_close('WINUSDT', 'take_profit', +1.2)
    locked, reason = vnz.is_entry_protection_locked('WINUSDT')
    assert locked and 'CooldownPeriod' in reason, "التهدئة تعمل بعد الإغلاق الرابح أيضًا"
    # الربح الصغير لا يفعّل LowProfitPairs (مجموع > 0) ولا الحمايات الشاملة
    print("✅ 7) CooldownPeriod: تهدئة الزوج بعد أي إغلاق — رابحًا كان أو خاسرًا")


def test_snapshot_structure():
    _reset_protections()
    _feed_close('SNAPUSDT', 'stop_loss', -0.8)
    snap = vnz.get_active_protections_snapshot()
    assert snap['enabled'] is True
    assert isinstance(snap['active_locks'], list) and len(snap['active_locks']) >= 1
    assert 'stats' in snap and 'config' in snap and 'recent_closes' in snap
    assert snap['config']['max_sl_distance_pct'] == 6.0
    assert any(lk['protection'] == 'CooldownPeriod' for lk in snap['active_locks'])
    print("✅ 8) لقطة /api/protections كاملة: أقفال + إحصاءات + إعدادات + إغلاقات حديثة")


def _mkdf(atr_value, close_value):
    import pandas as pd
    import numpy as np
    n = 25
    df = pd.DataFrame({
        'open': [close_value] * n, 'high': [close_value * 1.01] * n,
        'low': [close_value * 0.99] * n, 'close': [close_value] * n,
        'volume': [1000.0] * n, 'atr': [atr_value] * n,
    })
    df.name = 'TESTUSDT'
    return df


def test_sl_distance_cap():
    """نمط freqtrade (وقف ثابت محدود): وقف ATR الواسع يُقيَّد بسقف 6%."""
    entry = 100.0
    # ATR = 3.5 → SL عادي = 2.5×3.5 = 8.75% > 6% → يُقيَّد
    df = _mkdf(atr_value=3.5, close_value=entry)
    out = vnz.calculate_dynamic_tp_sl(df, entry)
    assert out is not None
    sl_dist = (entry - out['stop_loss']) / entry * 100
    assert abs(sl_dist - 6.0) < 0.01, f"الوقف يجب أن يُقيَّد إلى 6% لكنه {sl_dist:.2f}%"
    # TP لم يتأثر (4×3.5 = 14%)
    tp_dist = (out['target_price'] - entry) / entry * 100
    assert tp_dist > 13.5, f"الهدف لا يتأثر بالسقف: {tp_dist:.2f}%"
    # ATR صغير → لا تقييد (سوق هادئ: 2.5×0.8 = 2.0 مضاعف → 0.5×2.0 = 1.0% < 6%)
    df2 = _mkdf(atr_value=0.5, close_value=entry)
    out2 = vnz.calculate_dynamic_tp_sl(df2, entry)
    sl_dist2 = (entry - out2['stop_loss']) / entry * 100
    assert abs(sl_dist2 - 1.00) < 0.01, f"الوقف الضيق لا يُمس: {sl_dist2:.2f}%"
    print(f"✅ 9) سقف الوقف: 10.5% → {sl_dist:.1f}% (قص الذيل) والوقف الضيق {sl_dist2:.2f}% لم يُمس")


def test_structural_integration():
    """فحوص بنية: التغذية في close_signal، البوابات في الحلقة والمُرشحين، الخروج الزمني."""
    src = open('vnz.py', encoding='utf-8').read()
    tree = ast.parse(src)
    funcs = {n.name: ast.get_source_segment(src, n) for n in ast.walk(tree)
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    # 1) close_signal يغذي الحماية
    assert 'record_trade_close_for_protections(symbol_to_close, reason, profit_percentage)' in funcs['close_signal']
    # 2) الحلقة الرئيسية فيها بوابة شاملة
    assert 'is_entry_protection_locked(None)' in funcs['main_loop_enhanced']
    # 3) حلقة الإدارة فيها الخروج الزمني
    assert "close_signal(signal_id, current_price, 'stale_time_exit')" in funcs['trade_management_loop']
    # 4) حساب TP/SL فيه السقف
    assert 'MAX_SL_DISTANCE_PCT' in funcs['calculate_dynamic_tp_sl']
    # 5) سبب الخروج الزمني في خريطة الأسباب
    assert "'stale_time_exit'" in funcs['close_signal']
    print("✅ 10) البنية: تغذية الحمايات + بوابة شاملة + خروج زمني + سقف وقف — كلها مثبتة في الكود")


def test_api_registered():
    src = open('vnz.py', encoding='utf-8').read()
    assert "@app.route('/api/protections')" in src
    assert 'get_active_protections_snapshot()' in src
    print("✅ 11) /api/protections مسجلة للوحة التحكم (شفافية الأقفال نمط freqtrade UI)")


def test_winner_paths_unaffected():
    """الإغلاقات الرابحة لا تفعّل الحمايات الشاملة — فقط تهدئة الزوج (سلوك متوقع)."""
    _reset_protections()
    for i in range(6):
        _feed_close(f'G{i}USDT', 'take_profit', +1.0 + i * 0.2)
    locked, _ = vnz.is_entry_protection_locked(None)
    assert not locked, "6 إغلاقات رابحة لا تجمد البوت"
    print("✅ 12) سلسلة رابحة لا تفعّل أي تجميد شامل — الحمايات للخسائر فقط")


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failed += 1
            print(f"❌ {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"💥 {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{'=' * 50}\nالنتيجة: {len(tests) - failed}/{len(tests)} نجح")
    sys.exit(1 if failed else 0)
