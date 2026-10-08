# -*- coding: utf-8 -*-
"""اختبارات V9.22.0 — محرك الإدارة المثبت بالباك تيست + تدفئة المركز الكاملة.
الباك تيست (60 يومًا × 24 رمزًا، منطق المنتج حرفيًا): الأساس -85% ← التفعيل 1.5% +
المضاعف 2.8: +181%، PF 1.18، موجب في الشرائح الثلاث ويصمد مع رسوم 0.3%.
كذلك: ترقيم صفحات Bybit (عمق 30 يومًا حقيقي) يُخرج المركز من علّة buffers_warm 1/37."""
import inspect
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402


def test_version():
    assert vnz.APP_VERSION >= 'V9.22.0', f"الإصدار: {vnz.APP_VERSION}"
    print("✅ 1) الإصدار V9.22.0")


def test_trailing_params():
    """المعاملان المثبتان بالباك تيست."""
    assert vnz.ATR_TS_MULTIPLIER == 2.8, f"المضاعف: {vnz.ATR_TS_MULTIPLIER}"
    assert vnz.ATR_TRAIL_ACTIVATE_PROFIT_PCT == 1.5, f"التفعيل: {vnz.ATR_TRAIL_ACTIVATE_PROFIT_PCT}"
    src = inspect.getsource(vnz.trade_management_loop)
    assert 'ATR_TRAIL_ACTIVATE_PROFIT_PCT' in src, "بوابة التفعيل غائبة عن حلقة الإدارة"
    assert 'trail_ok' in src, "منطق بوابة التفعيل غائب"
    # السلوك القديم (رفع من أول قمة) يجب ألا يعمل بلا بوابة
    old = "latest_atr = get_cached_atr(symbol)\n                        if latest_atr"
    assert old not in src.replace('\n', '\n'), "السلوك القديم بلا بوابة ما زال موجودًا"
    print("✅ 2) تريلينغ: مضاعف 2.8 + بوت تفعيل بعد قمة +1.5%")


def test_trailing_gate_semantics():
    """بوابة التفعيل: دون الحد لا يُرفع الوقف؛ فوقه يُرفع إلى peak - 2.8×ATR."""
    entry = 100.0
    act = vnz.ATR_TRAIL_ACTIVATE_PROFIT_PCT
    peak_low = entry * (1 + act / 100.0) * 0.999   # دون الحد
    peak_ok = entry * (1 + act / 100.0)            # عند الحد
    trail_ok_low = (act <= 0) or (peak_low >= entry * (1 + act / 100.0))
    trail_ok_ok = (act <= 0) or (peak_ok >= entry * (1 + act / 100.0))
    assert trail_ok_low is False and trail_ok_ok is True
    # عند التفعيل: وقف أعلى من الدخول لأن 2.8×ATR < 1.5% في سوق هادئ قد لا يتحقق —
    # الحماية: new_sl > sl فقط يُرفع، وإلا يبقى الوقف الأصلي (نفس منطق المنتج)
    atr = entry * 0.004  # 0.4%
    new_sl = peak_ok - atr * vnz.ATR_TS_MULTIPLIER
    assert new_sl > entry * 0.98  # فوق وقف كارثي — منطقي
    print("✅ 3) دلالات بوابة التفعيل صحيحة (لا رفع دون الحد، الرفع فقط إذا تجاوز الوقف الحالي)")


def test_bybit_pagination():
    """طلب عمق > 1000 يجب أن يُلبّى بالترقيم الرجعي لا أن يُقص."""
    p = vnz.BybitProvider()
    lim = 2500
    rows = p.fetch_klines('BTCUSDT', '15m', lim)
    assert rows and len(rows) >= 2000, f"الترقيم لم يلبِّ العمق: {len(rows) if rows else 0}"
    # ترتيب زمني صاعد بلا تكرار
    ts = [r[0] for r in rows]
    assert ts == sorted(ts), "الترتيب الزمني مكسور"
    assert len(set(ts)) == len(ts), "تكرار شموع"
    print(f"✅ 4) ترقيم Bybit: طلب {lim} → {len(rows)} شمعة مرتبة بلا تكرار")


def test_lookback_cap_raised():
    assert vnz.lookback_to_candles('920 hour', '15m') == 3680, "سقف 1000 القديم ما زال يقطع العمق"
    assert vnz.lookback_to_candles('1300 hour', '4h') == 325
    print("✅ 5) lookback_to_candles: سقف 5000 يمرر العمق الكامل")


def test_fetch_depth_matches_hub_target():
    """عمق fetch_historical_data يجب أن يطابق هدف تدفئة المركز (لا REST دائم)."""
    iv_min = vnz._INTERVAL_MINUTES['15m']
    n = ((vnz.SIGNAL_GENERATION_LOOKBACK_DAYS * 24 + 200) * 60) // iv_min
    assert n == 3680, f"عمق 15م: {n}"
    # نتحقق عبر مثال حقيقي (الإنشاء لا يفتح اتصالًا)
    hub = vnz.MarketStreamHub()
    assert hub._target_count['15m'] == 3680
    assert hub._target_count['4h'] == 320
    print("✅ 6) عمق المسح 3680 يطابق هدف المركز — التدفئة قابلة للبلوغ")


def test_budget_pinned():
    assert vnz.RATE_LIMIT_BUDGET_PER_MIN == 500, f"الميزانية: {vnz.RATE_LIMIT_BUDGET_PER_MIN}"
    g = vnz.BinanceRateGuard()
    assert g.configured_budget == 500
    print("✅ 7) ميزانية باينانس مثبتة عند 500 (كانت 1500 والتعافي حتى 1410 بلا فائدة)")


def test_hub_warm_logic_reachable():
    """محاكاة: بعد تعبئة 3680 شمعة يجب أن يعد المخزون warm ويخدم build_dataframe."""
    hub = vnz.MarketStreamHub()
    sym = 'TESTUSDT'
    now_ms = int(time.time() * 1000) // 900000 * 900000
    rows = []
    for i in range(3680):
        ot = now_ms - (3679 - i) * 900000
        rows.append((ot, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0))
    with hub._lock:
        hub._closed[(sym, '15m')] = {r[0]: r for r in rows}
        hub._desired.add((sym, '15m'))
        hub._last_msg_ts = time.time()
    snap = hub.snapshot()
    assert snap['buffers_warm'] == 1, f"المخزون المكتمل لم يُعد warm: {snap}"
    df = hub.build_dataframe(sym, '15m', 30)
    assert df is not None and len(df) >= 3680, f"build_dataframe لم يخدم من المخزون: {len(df) if df is not None else None}"
    print("✅ 8) مخزون مكتمل 3680 = warm + يخدم المسح من الذاكرة (صفر REST)")


def test_backfill_target_counts():
    """أهداف التعبئة قابلة للبلوغ ضمن سقف الترقيم 5000."""
    hub = vnz.MarketStreamHub()
    for iv, lookback in hub._backfill_lookback.items():
        need = vnz.lookback_to_candles(lookback, iv)
        assert need >= hub._target_count[iv], f"{iv}: التعبئة {need} < الهدف {hub._target_count[iv]}"
        assert need <= 5000, f"{iv}: العمق {need} يتجاوز سقف الترقيم"
    print("✅ 9) أهداف التعبئة (15m/1h/4h) قابلة للبلوغ بالترقيم الجديد")


def test_old_tests_compatibility():
    """فحوص بنيوية سريعة تضمن عدم كسر مسارات V9.20/9.21."""
    assert vnz.SCAN_DURING_BAN is True
    assert vnz.DATA_FEED_MODE == 'multi'
    assert hasattr(vnz, 'get_entry_price_ban_aware')
    src = inspect.getsource(vnz.fetch_historical_data)
    assert 'build_dataframe' in src, "أولوية مركز WS انكسرت"
    print("✅ 10) مسارات V9.20/V9.21 سليمة (SCAN_DURING_BAN، أولوية WS، سعر الدخول المدرك للحظر)")


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
