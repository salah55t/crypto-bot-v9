# -*- coding: utf-8 -*-
"""اختبارات V9.21.0 — استقلال طائرة البيانات عن حظر باينانس.
المبدأ: المسح والبوصلة والقادة يواصلون العمل عبر مركز WS والمزودين البديلين أثناء
حظر باينانس، ويُؤجَّل التنفيذ الحقيقي فقط. تُجرى بلا شبكة وكل النداءات موكّاة."""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402


def test_version_and_dashboard():
    # [V9.22.0] مرن عبر الإصدارات: V9.* — الرقم المصدر APP_VERSION نفسه
    import re as _re
    assert _re.match(r'^V9\.\d+\.\d+$', vnz.APP_VERSION), f"الإصدار: {vnz.APP_VERSION}"
    html = vnz.get_dashboard_html()
    assert vnz.APP_VERSION in html and 'sys-exec' in html
    print(f"✅ 1) الإصدار {vnz.APP_VERSION} + شارة حالة التنفيذ في شريط المراقبة")


def test_scan_during_ban_config():
    assert vnz.SCAN_DURING_BAN is True, "الافتراضي: المسح مستمر أثناء الحظر"
    print("✅ 2) مفتاح SCAN_DURING_BAN موجود وافتراضيه True")


def test_main_loop_no_full_halt():
    """بوابة الحظر في الحلقة الرئيسية لم تعد تُجمّد الدورة كاملة."""
    import inspect
    src = inspect.getsource(vnz.main_loop_enhanced)
    assert 'exec_blocked = ban_remain > 0' in src, "يجب حساب exec_blocked بدل continue الفوري"
    assert 'SCAN_DURING_BAN' in src
    # السلوك القديم: أول سطر بعد فحص الحظر كان continue — يجب ألا يحدث إلا مع SCAN_DURING_BAN=False
    old_gate = "if ban_remain > 0:\n                logger.warning"
    assert old_gate not in src, "بوابة V9.9 القديمة ما زالت توقف الدورة كاملة"
    assert 'get_entry_price_ban_aware' in src, "سعر الدخول يجب أن يكون بوعي الحظر"
    print("✅ 3) الحلقة الرئيسية: المسح مستمر أثناء الحظر والتنفيذ وحده المؤجل")


def test_entry_price_ban_aware_fallbacks():
    """سعر الدخول: باينانس عند السماح ← مركز WS ← المزودون البديلون."""
    # 1) أثناء الحظر: مركز WS يوفر السعر (سعر باينانص حي بلا وزن)
    class FakeHub:
        def get_prices(self, symbols):
            return {'BTCUSDT': 84000.5}
    saved_hub = vnz.stream_hub
    vnz.stream_hub = FakeHub()
    try:
        p = vnz.get_entry_price_ban_aware('BTCUSDT', exec_blocked=True)
        assert p == 84000.5, f"مركز WS كان يجب أن يوفر السعر أثناء الحظر: {p}"
    finally:
        vnz.stream_hub = saved_hub
    print("✅ 4) أثناء الحظر: سعر الدخول من مركز WebSocket")


def test_entry_price_from_datafeed_when_hub_cold():
    """مركز WS بارد → المزود البديل (إغلاق شمعة 1م الجارية)."""
    class ColdHub:
        def get_prices(self, symbols):
            return {}
    class FakeFeed:
        def get_klines(self, symbol, interval, limit=300):
            assert interval == '1m'
            return [[0, 1, 2, 1, 99.25, 1, 0, 0, 0, 0, 0, 0],
                    [0, 1, 2, 1, 99.75, 1, 0, 0, 0, 0, 0, 0]]
    saved_hub, saved_feed = vnz.stream_hub, vnz.data_feed
    vnz.stream_hub, vnz.data_feed = ColdHub(), FakeFeed()
    try:
        p = vnz.get_entry_price_ban_aware('NMRUSDT', exec_blocked=True)
        assert p == 99.75, f"إغلاق آخر شمعة 1م كان متوقعًا: {p}"
    finally:
        vnz.stream_hub, vnz.data_feed = saved_hub, saved_feed
    print("✅ 5) مركز WS بارد → إغلاق شمعة 1م من المزود البديل")


def test_entry_price_none_when_all_fail():
    """فشل الجميع → None (تُتخطى الإشارة بدل تعليق الخيط ساعات)."""
    class DeadHub:
        def get_prices(self, symbols):
            return {}
    class DeadFeed:
        def get_klines(self, symbol, interval, limit=300):
            return None
    saved_hub, saved_feed = vnz.stream_hub, vnz.data_feed
    vnz.stream_hub, vnz.data_feed = DeadHub(), DeadFeed()
    try:
        p = vnz.get_entry_price_ban_aware('XXXXUSDT', exec_blocked=True)
        assert p is None
    finally:
        vnz.stream_hub, vnz.data_feed = saved_hub, saved_feed
    print("✅ 6) فشل كل المصادر أثناء الحظر → None بلا تعليق")


def test_background_loops_ban_free():
    """بوصلة BTC وخريطة القادة: بوابات الحظر أُزيلت — مصادرها محصنة أصلاً."""
    import inspect
    btc_src = inspect.getsource(vnz.btc_trend_loop)
    lead_src = inspect.getsource(vnz.leader_data_loop)
    assert 'banned_until' not in btc_src, "بوصلة BTC ما زالت تتجمد أثناء الحظر"
    assert 'banned_until' not in lead_src, "خريطة القادة ما زالت تتجمد أثناء الحظر"
    # خيط الرصيد يبقى بوابقته — بيانات طائرة التنفيذ (REST محسوب لا يعمل أثناء الحظر)
    bal_src = inspect.getsource(vnz.balance_refresh_loop)
    assert 'banned_until' in bal_src, "خيط الرصيد (طائرة التنفيذ) يحتفظ بوابقته الصحيحة"
    print("✅ 7) البوصلة والقادة حرّة من بوابة الحظر، وخيط الرصيد يحتفظ بها (طائرة التنفيذ)")


def test_real_trade_deferral_logic():
    """التداول الحقيقي أثناء الحظر: يُؤجَّل ولا صفقة ورقية توهم موقعًا."""
    import inspect
    src = inspect.getsource(vnz.main_loop_enhanced)
    assert 'if is_enabled and exec_blocked:' in src
    assert 'Execution Deferred (Binance Ban)' in src
    print("✅ 8) التنفيذ الحقيقي مؤجل أثناء الحظر مع توثيق سبب الرفض")


def test_system_status_exposes_exec_state():
    """واجهة الحالة تكشف exec_blocked وscan_during_ban للوحة."""
    import inspect
    src = inspect.getsource(vnz.api_system_status)
    assert "'exec_blocked'" in src and "'scan_during_ban'" in src
    print("✅ 9) /api/system_status يعرض حالة التنفيذ للوحة")


def test_rest_fallbacks_ban_guarded():
    """احتياطا REST (شموع/دفتر أوامر) لا يُلامسان أثناء الحظر — منع تعليق خيط المسح."""
    import inspect
    fh_src = inspect.getsource(vnz.fetch_historical_data)
    ob_src = inspect.getsource(vnz.passes_final_order_book_check)
    assert 'banned_until' in fh_src, "جلب الشموع يحتاج حراسة الحظر على احتياط REST"
    assert 'banned_until' in ob_src, "دفتر الأوامر يحتاج حراسة الحظر على احتياط REST"
    print("✅ 11) احتياطا REST للشموع ودفتر الأوامر محروسان بالحظر (لا تعليق خيوط)")


def test_rate_guard_snapshot_unchanged():
    """الحارس نفسه لم يُمس — الحظر يبقى مسجلاً بموعد انتهاء دقيق."""
    snap = vnz.rate_guard.snapshot()
    for key in ('banned_until', 'ban_remaining_sec', 'budget_per_min', 'used_weight_last_min'):
        assert key in snap
    print("✅ 10) حارس الطلبات سليم: الحظر يُسجل ويُقدم للوحة كما هو")


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    failed = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            failed += 1
            print(f"❌ {t.__name__}: {e}")
    print(f"\n{'='*50}\nالنتيجة: {len(tests) - failed}/{len(tests)} نجح")
    sys.exit(1 if failed else 0)
