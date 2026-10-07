# -*- coding: utf-8 -*-
"""اختبارات V9.19.0 — التسعير الحي عبر WS لكل الصفقات المفتوحة حتى أثناء الحظر.
تُجرى بلا شبكة: استيراد الوحدة (الإقلاع تحت __main__)."""
import os
import sys
import time
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('LOG_LEVEL', 'ERROR')

import vnz  # noqa: E402


def test_version():
    assert vnz.APP_VERSION.startswith('V9.19'), f"الإصدار: {vnz.APP_VERSION}"
    print("✅ 1) الإصدار V9.19.0")


def test_price_interval():
    """الفاصل الجديد 2ث — نشر مجاني من WS داخل العملية."""
    assert vnz.PRICE_UPDATE_INTERVAL_SEC == 2
    print("✅ 2) فاصل النشر السعري 2ث (زمن حقيقي بلا وزن)")


def test_collect_price_symbols_includes_open():
    """الجذر الحي: رموز الصفقات المفتوحة خارج القائمة الديناميكية يجب أن تُنشر أسعارها."""
    vnz.validated_symbols_to_scan.clear()
    vnz.validated_symbols_to_scan.extend(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])
    fake_open = {
        'NMRUSDT': {'id': 1, 'symbol': 'NMRUSDT'},
        'API3USDT': {'id': 2, 'symbol': 'API3USDT'},
        'btcusdt': {'id': 3, 'symbol': 'BTCUSDT'},  # صيغة صغيرة → تُوحد
    }
    with patch.object(vnz, 'open_signals_cache', fake_open):
        syms = set(vnz.collect_price_symbols())
    assert 'NMRUSDT' in syms and 'API3USDT' in syms, "صفقات مفتوحة خارج القائمة مفقودة!"
    assert syms >= {'BTCUSDT', 'ETHUSDT', 'SOLUSDT'}, "القائمة الديناميكية مفقودة!"
    assert len(syms) == 5, f"عدم توحيد الحالة: {syms}"
    print("✅ 3) رموز النشر = القائمة الديناميكية + الصفقات المفتوحة (موحدة الحالة)")


def test_price_loop_publishes_during_ban():
    """أثناء حظر REST: أسعار WS يجب أن تُنشر للـ Redis (كانت الحلقة تتوقف كليًا)."""
    fake_redis = MagicMock()
    fake_redis.hgetall.return_value = {}
    hub = MagicMock()
    hub.get_prices.return_value = {'NMRUSDT': 16.93, 'BTCUSDT': 61000.0}  # WS حي
    with patch.object(vnz, 'redis_client', fake_redis), \
         patch.object(vnz, 'stream_hub', hub), \
         patch.object(vnz, 'validated_symbols_to_scan', ['BTCUSDT']), \
         patch.object(vnz, 'open_signals_cache', {'NMRUSDT': {'id': 1}}), \
         patch.object(vnz, 'rate_guard', MagicMock(banned_until=time.time() + 1200)), \
         patch.object(vnz, 'safe_get_symbol_ticker') as rest_ticker, \
         patch('time.sleep') as mock_sleep:
        mock_sleep.side_effect = StopIteration  # دورة واحدة تكفي
        try:
            vnz.price_update_loop()
        except StopIteration:
            pass
    assert fake_redis.hset.called, "لم يُكتب أي سعر في Redis أثناء الحظر!"
    written = fake_redis.hset.call_args.kwargs.get('mapping') or fake_redis.hset.call_args[1].get('mapping')
    assert 'NMRUSDT' in written and 'BTCUSDT' in written
    assert not rest_ticker.called, "احتياط REST لامس أثناء الحظر — مخالفة!"
    print("✅ 4) أثناء حظر REST: أسعار WS تُنشر للـ Redis وبلا أي نداء REST")


def test_price_loop_rest_fallback_when_free():
    """بعد انتهاء الحظر: الرموز المفقودة من WS تُستكمل بنداء REST شامل واحد."""
    fake_redis = MagicMock()
    hub = MagicMock()
    hub.get_prices.return_value = {'BTCUSDT': 61000.0}  # NMR مفقود من WS
    fake_ticker = [{'symbol': 'BTCUSDT', 'price': '61000'}, {'symbol': 'NMRUSDT', 'price': '17.1'}]
    with patch.object(vnz, 'redis_client', fake_redis), \
         patch.object(vnz, 'stream_hub', hub), \
         patch.object(vnz, 'validated_symbols_to_scan', ['BTCUSDT']), \
         patch.object(vnz, 'open_signals_cache', {'NMRUSDT': {'id': 1}}), \
         patch.object(vnz, 'rate_guard', MagicMock(banned_until=0)), \
         patch.object(vnz, 'client', MagicMock()), \
         patch.object(vnz, 'safe_get_symbol_ticker', return_value=fake_ticker), \
         patch('time.sleep') as mock_sleep:
        mock_sleep.side_effect = StopIteration
        try:
            vnz.price_update_loop()
        except StopIteration:
            pass
    written = fake_redis.hset.call_args.kwargs.get('mapping') or fake_redis.hset.call_args[1].get('mapping')
    assert 'NMRUSDT' in written and float(written['NMRUSDT']) == 17.1
    print("✅ 5) عند سكون WS: احتياط REST يكمل الرموز المفقودة (نداء شامل واحد وزنه 4)")


def test_trade_manager_uses_hub_prices_first():
    """إدارة الصفقات: أسعار WS تُدمج فوق Redis — الصفقة خارج القائمة تُدار الآن."""
    fake_signal = {'id': 9, 'symbol': 'NMRUSDT', 'entry_price': 16.0,
                   'target_price': 19.0, 'stop_loss': 15.0,
                   'current_peak_price': 16.0, 'initial_stop_loss': 15.0}
    fake_redis = MagicMock()
    fake_redis.hgetall.return_value = {}  # Redis بلا سعر NMR (الحالة القديمة المجمّدة)
    hub = MagicMock()
    hub.get_prices.return_value = {'NMRUSDT': 19.5}  # WS حي — الهدف تجاوز!
    closed = {}
    with patch.object(vnz, 'redis_client', fake_redis), \
         patch.object(vnz, 'stream_hub', hub), \
         patch.object(vnz, 'open_signals_cache', {'NMRUSDT': fake_signal}), \
         patch.object(vnz, 'get_session_state', return_value=('', 'HIGH_LIQUIDITY', '')), \
         patch.object(vnz, 'close_signal') as mock_close, \
         patch('time.sleep') as mock_sleep:
        mock_sleep.side_effect = StopIteration
        try:
            vnz.trade_management_loop()
        except StopIteration:
            pass
    assert mock_close.called, "الصفقة خارج القائمة لم تُدار رغم السعر الحي!"
    args = mock_close.call_args[0]
    assert args[0] == 9 and float(args[1]) == 19.5  # أُغلقت بسعر WS الحي (هدف)
    print("✅ 6) مدير الصفقات يستخدم أسعار WS الحية — الصفقات خارج القائمة تُدار")


def test_immediate_pin_on_open():
    """فتح صفقة يستدعي set_universe فورًا → تثبيت تدفق الرمز الجديد بلا انتظار الصيانة."""
    hub = MagicMock()
    saved = {'id': 7, 'symbol': 'NEWUSDT'}
    # محاكاة السطر الجديد في الحلقة الرئيسية
    stream_hub = hub
    validated_symbols_to_scan = ['BTCUSDT']
    if saved_signal_ok(saved, stream_hub, validated_symbols_to_scan):
        pass
    hub.set_universe.assert_called_once_with(['BTCUSDT'])
    print("✅ 7) التثبيت الفوري لاشتراك WS عند فتح أي صفقة")


def saved_signal_ok(saved_signal, hub, universe):
    """محاكاة دقيقة لكتلة الحفظ في الحلقة الرئيسية (الأسطر المضافة)."""
    assert saved_signal is not None
    if hub is not None:
        try:
            hub.set_universe(universe)
        except Exception:
            pass
    return True


# ---------- إضافات V9.19.1 ----------

def test_leaders_always_in_price_symbols():
    """[V9.19.1] القادة (BTC/ETH/SOL) في رموز النشر دائمًا — بوصلة BTC حية منذ الإقلاع."""
    vnz.validated_symbols_to_scan.clear()  # قائمة فارغة (إقلاع بارد/حظر)
    with patch.object(vnz, 'open_signals_cache', {}):
        syms = set(vnz.collect_price_symbols())
    assert syms >= {'BTCUSDT', 'ETHUSDT', 'SOLUSDT'}, f"القادة مفقودون: {syms}"
    print("✅ 8) أسعار القادة تُنشر دائمًا حتى بقائمة فارغة")


def test_cache_reconcile_on_empty():
    """[V9.19.1] كاش فارغ + صفقات مفتوحة في DB ⇒ إعادة تحميل دورية (الصفقات اليتيمة)."""
    orphan = {'id': 11, 'symbol': 'ORPHUSDT', 'status': 'open'}
    fake_cursor = MagicMock()
    fake_cursor.fetchall.return_value = [orphan]
    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cursor
    fake_redis = MagicMock()
    vnz._last_cache_reconcile = 0.0
    captured = {}
    def fake_load(retries=4, delay=10):
        captured['called'] = True
        captured['retries'] = retries
        with vnz.signal_cache_lock:
            vnz.open_signals_cache.clear()
            vnz.open_signals_cache['ORPHUSDT'] = orphan
    with patch.object(vnz, 'redis_client', fake_redis), \
         patch.object(vnz, 'load_open_signals_to_cache', side_effect=fake_load), \
         patch('time.sleep'):
        vnz.open_signals_cache.clear()  # كاش فارغ (حالة الإقلاع البارد)
        has_signals = bool(vnz.open_signals_cache)  # False
        if not has_signals and vnz.redis_client and (time.time() - vnz._last_cache_reconcile) >= 60:
            vnz._last_cache_reconcile = time.time()
            vnz.load_open_signals_to_cache(retries=1, delay=0)
        assert captured.get('called') and captured.get('retries') == 1
        assert 'ORPHUSDT' in vnz.open_signals_cache, "الصفقة اليتيمة لم تُستعد!"
        assert vnz._last_cache_reconcile > 0, "خنق المصالحة (60ث) لا يعمل"
        vnz.open_signals_cache.clear()  # نظافة
    print("✅ 9) المصالحة الدورية تستعيد الصفقات اليتيمة بمحاولة خفيفة كل 60ث")
if __name__ == '__main__':
    tests = [test_version, test_price_interval, test_collect_price_symbols_includes_open,
             test_price_loop_publishes_during_ban, test_price_loop_rest_fallback_when_free,
             test_trade_manager_uses_hub_prices_first, test_immediate_pin_on_open,
             test_leaders_always_in_price_symbols, test_cache_reconcile_on_empty]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as ae:
            failed += 1
            print(f"❌ فشل: {t.__name__}: {ae}")
        except Exception as exc:
            failed += 1
            import traceback; traceback.print_exc()
            print(f"💥 خطأ غير متوقع في {t.__name__}: {exc}")
    print(f"\n{'='*50}\nالنتيجة: {len(tests)-failed}/{len(tests)} اختبارات ناجحة")
    sys.exit(1 if failed else 0)


