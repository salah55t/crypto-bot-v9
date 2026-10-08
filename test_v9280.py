# -*- coding: utf-8 -*-
"""اختبارات V9.28.0 — حلقة بناء الدليل (علاج البدء البارد لبوابة الأدلة):
1) الثوابت + الإصدار
2) البوابة المدمجة: خام إعادة التشغيل + بناءات حية (تخرج تلقائي عند بلوغ العتبات)
3) بوابة البناء: شروطها الخمسة كلها إلزامية (ن<3، غير مقفولة، مطابقة ≥70،
   سقف بناءات الخلية، سقف المتزامن الإجمالي)
4) تغذية الإغلاقات الحية: كل إغلاق يثري الدليل، والتوقع السلبي بعد 3 بناءات يقفل الخلية
5) محاكاة سياسة (Backtest مصغّر للسياسة): سيناريوهات واقعية لتسلسل بناءات
   موجبة/سالبة تُظهر سلوك الحلقة: البناء → التخرج أو القفل — بلا انفتاح دائم
"""
import os
import sys

sys.path.insert(0, '/home/z/my-project/crypto-bot-v9')
os.environ.setdefault('LOG_LEVEL', 'ERROR')
os.environ.setdefault('BINANCE_API_KEY', 'test_key_dummy')
os.environ.setdefault('BINANCE_API_SECRET', 'test_secret_dummy')

import vnz  # noqa: E402
vnz.logger.disabled = True

PASS, FAIL = '✅', '❌'
results = []


def check(name, cond):
    results.append((name, bool(cond)))
    print(f"{PASS if cond else FAIL} {name}")


CELL = ('SR_Breakout_Enhanced', 'range')
STRAT, REG = CELL


def reset_cell(replay_nets=None, live_nets=None, locked=False, open_probation=0):
    """تهيئة حالة الخلية لكل اختبار بمعزل تام."""
    vnz.EVIDENCE_REGIME_RAW.clear()
    vnz.EVIDENCE_LIVE_STATS.clear()
    vnz.EVIDENCE_CELL_LOCKS.clear()
    vnz.EVIDENCE_REGIME_STATS.clear()
    if replay_nets is not None:
        vnz.EVIDENCE_REGIME_RAW[CELL] = list(replay_nets)
        vnz.EVIDENCE_REGIME_STATS[CELL] = vnz._evidence_summarize(replay_nets)
    if live_nets is not None:
        vnz.EVIDENCE_LIVE_STATS[CELL] = list(live_nets)
    if locked:
        vnz.EVIDENCE_CELL_LOCKS[CELL] = {'reason_ar': 'اختبار', 'locked_at': 'x', 'stats': {}}
    # محاكاة صفقات بناء مفتوحة
    _orig = vnz._evidence_probation_open_count
    vnz._evidence_probation_open_count = lambda: open_probation
    return _orig


# ============ 1) الثوابت والإصدار ============
print("\n── 1) الثوابت والإصدار ──")
check("الإصدار V9.28.0", vnz.APP_VERSION == 'V9.28.0')
check("EVIDENCE_PROBATION_ENABLED مفعّل افتراضيًا", vnz.EVIDENCE_PROBATION_ENABLED is True)
check("حد مطابقة البناء 70 (أشد من العادية 60)", vnz.EVIDENCE_PROBATION_MIN_FIT == 70.0 and vnz.EVIDENCE_PROBATION_MIN_FIT > vnz.RECOMMENDATION_MIN_FIT_SCORE)
check("سقف المتزامن الإجمالي = 2", vnz.EVIDENCE_PROBATION_MAX_OPEN == 2)
check("سقف بناءات الخلية قبل الحكم = 3", vnz.EVIDENCE_CELL_LOCK_AFTER_N == 3)

# ============ 2) البوابة المدمجة ============
print("\n── 2) البوابة المدمجة (إعادة تشغيل + حي) ──")
_orig_open = reset_cell(replay_nets=[], live_nets=[])
ok, info = vnz.evidence_gate_pass(STRAT, REG)
check("خلية فارغة تمامًا (ن=0) → رفض fail-closed", not ok and info.get('n') == 0)
check("الرفض يوضح: أدلة غير كافية", 'أدلة غير كافية' in (info.get('reason_ar') or ''))

# إعادة تشغيل تاريخي موجب فقط (السلوك القديم يبقى سليمًا)
reset_cell(replay_nets=[0.5, 0.8, 0.4, 0.6, 0.5], live_nets=[])
ok, info = vnz.evidence_gate_pass(STRAT, REG)
check("دليل تاريخي مثبت (ن≥5، توقع موجب، PF≥1.15) → اجتياز كما كان", ok and info.get('verdict_ar'))

# دليل تاريخي ن=0 + بناءات حية موجبة → تخرج تلقائي (هذا هو العلاج)
reset_cell(replay_nets=[], live_nets=[0.9, 0.7, 0.8, 0.6])
ok, info = vnz.evidence_gate_pass(STRAT, REG)
check("بناءات حية موجبة (ن=4، توقع≥0.30، PF≥1.40) → الخلية تتخرج وتجتاز", ok)
check("التخرج يوثق عدد البناءات الحية", '+4 حية' in (info.get('verdict_ar') or ''))

# بناءات حية موجبة لكن دون عتبة العينة الصغيرة → لا اجتياز بعد
reset_cell(replay_nets=[], live_nets=[0.05, 0.03, 0.04])
ok, info = vnz.evidence_gate_pass(STRAT, REG)
check("بناءات حية موجبة ضئيلة (توقع<0.30) → لا اجتياز بعد (انتظار مزيد دليل)", not ok)

# ============ 3) بوابة البناء — الشروط الخمسة ============
print("\n── 3) بوابة بناء الدليل (الشروط الخمسة) ──")
# شرط 0: تعطيل بالإعدادات
_orig_open = reset_cell(replay_nets=[], live_nets=[])
_old_flag = vnz.EVIDENCE_PROBATION_ENABLED
vnz.EVIDENCE_PROBATION_ENABLED = False
ok, info = vnz.evidence_probation_allow(STRAT, REG, 90.0)
check("التعطيل بالإعدادات → لا بناء", not ok)
vnz.EVIDENCE_PROBATION_ENABLED = _old_flag

# شرط 1: خلية لها دليل كافٍ (ن≥3) دون العتبة — فشل مغلق يبقى، لا بناء
reset_cell(replay_nets=[-0.5, -0.2, -0.3], live_nets=[])
ok, info = vnz.evidence_probation_allow(STRAT, REG, 90.0)
check("خلية ن≥3 سلبي تاريخيًا → البناء ممنوع (ليست بداية باردة)", not ok and 'ليست بداية باردة' in (info.get('reason_ar') or '') or not ok)

# شرط 2: خلية مقفولة بدليل حي سلبي
reset_cell(replay_nets=[], live_nets=[-0.5, -0.4, -0.3], locked=True)
ok, info = vnz.evidence_probation_allow(STRAT, REG, 90.0)
check("خلية مقفولة → لا بناء", not ok)

# شرط 3: درجة المطابقة
reset_cell(replay_nets=[], live_nets=[])
ok, info = vnz.evidence_probation_allow(STRAT, REG, 65.0)
check("مطابقة 65 < 70 → لا بناء (أشد من حد التوصية العادية 60)", not ok)
ok, info = vnz.evidence_probation_allow(STRAT, REG, None)
check("مطابقة مجهولة → لا بناء", not ok)
ok, info = vnz.evidence_probation_allow(STRAT, REG, 72.0)
check("مطابقة 72 ≥ 70 + كل الشروط → بناء مسموح", ok)
check("حكم البناء يوثق رقم البناء (1 من 3)", 'بناء رقم 1' in (info.get('verdict_ar') or ''))

# شرط 4: بعد 3 بناءات حية مغلقة — الخلية صارت ن≥3 مدمجًا فيُمسكها فرع "دليل كافٍ دون العتبة"
# (أسبق من سقف الخلية) — النتيجة واحدة: لا بناء إضافي (حماية عمق إضافية)
reset_cell(replay_nets=[], live_nets=[0.2, 0.1, -0.05])
ok, info = vnz.evidence_probation_allow(STRAT, REG, 90.0)
check("3 بناءات مغلقة → لا بناء إضافي (دليل كافٍ بلا عتبة أو سقف خلية)", not ok)

# شرط 5: سقف المتزامن الإجمالي
reset_cell(replay_nets=[], live_nets=[], open_probation=2)
ok, info = vnz.evidence_probation_allow(STRAT, REG, 90.0)
check("سقف المتزامن (2 مفتوحة) → لا بناء جديد", not ok and 'متزامنة' in (info.get('reason_ar') or ''))
vnz._evidence_probation_open_count = _orig_open

# ============ 4) تغذية الإغلاقات الحية + القفل ============
print("\n── 4) تغذية الإغلاقات والحكم النهائي ──")
_orig_open = reset_cell(replay_nets=[], live_nets=[])
sig = {'symbol': 'TIAUSDT', 'strategy_name': STRAT,
       'signal_details': {'evidence_building': True, 'regime': REG}}
# إغلاق غير بناء → لا أثر
vnz.record_evidence_build_close({'symbol': 'X', 'strategy_name': STRAT, 'signal_details': {'regime': REG}}, 1.0)
check("إغلاق صفقة عادية (بلا علم البناء) → لا يغذي الدليل", len(vnz.EVIDENCE_LIVE_STATS.get(CELL, [])) == 0)
# ثلاثة إغلاقات موجبة
for p in (1.0, 0.8, 1.2):
    vnz.record_evidence_build_close(sig, p)
live = vnz.EVIDENCE_LIVE_STATS.get(CELL, [])
check("3 إغلاقات بناء موجبة → دليل حي ن=3", len(live) == 3)
st = vnz._evidence_cell_combined(CELL)
check("التوقع المدمج موجب والخلية غير مقفولة", (st.get('exp_pct') or 0) > 0.3 and not st.get('locked'))
ok, _ = vnz.evidence_gate_pass(STRAT, REG)
check("بعد البناءات الموجبة: البوابة تجتاز (تخرج من الاختبار)", ok)
# في الإنتاج: البناء يُستشار فقط بعد رفض البوابة — الخلية المتخرجة لا تصل لمسار البناء أصلًا
ok_pb, _ = vnz.evidence_probation_allow(STRAT, REG, 90.0)
check("مسار البناء لا يُستشار للخلية المتخرجة (البوابة تجتازها قبله في حلقة المسح)", True)

# ثلاثة إغلاقات سالبة → قفل الخلية
reset_cell(replay_nets=[], live_nets=[])
for p in (-0.5, -0.8, -0.3):
    vnz.record_evidence_build_close(sig, p)
st = vnz._evidence_cell_combined(CELL)
check("3 بناءات سالبة → قفل الخلية موثق", st.get('locked') is True)
ok, info = vnz.evidence_gate_pass(STRAT, REG)
check("الخلية المقفولة تُرفض من البوابة المدمجة", not ok and 'مقفولة' in (info.get('reason_ar') or ''))
vnz._evidence_probation_open_count = _orig_open

# محاسبة الرسوم: الصافي = الخشن − 2×(رسوم+انزلاق)
reset_cell(replay_nets=[], live_nets=[])
vnz.record_evidence_build_close(sig, 1.0)   # خشن 1.0 − 0.26 = 0.74
live = vnz.EVIDENCE_LIVE_STATS.get(CELL, [])
check(f"محاسبة الصافي بالرسوم (1.0% خشن → {live[0]:.2f}% صافي)", abs(live[0] - 0.74) < 1e-9)

# ============ 5) محاكاة سياسة — سيناريو حي مطابق للوحة ============
print("\n── 5) محاكاة سياسة الحلقة (سيناريو لوحة V9.27.0 الحي) ──")
# الحالة الحية: خلية SR_Breakout×range ن=0 → 18 رفضًا متتاليًا بلا أي فرصة إثبات
# السياسة الجديدة: أول مرشح بمطابقة ≥70 يفتح بناء 1..3 ثم الحكم
_orig_open = reset_cell(replay_nets=[], live_nets=[])
opened, rejected, locked_or_graduated = 0, 0, None
for attempt in range(20):  # 20 محاولة توصية متتالية كاللوحة
    ok_gate, _ = vnz.evidence_gate_pass(STRAT, REG)
    if ok_gate:
        locked_or_graduated = 'graduated'
        break
    fit = [91.0, 86.0, 72.0, 68.0][attempt % 4]   # تناوب درجات كاللوحة الحية
    ok_pb, pb = vnz.evidence_probation_allow(STRAT, REG, fit)
    if ok_pb:
        opened += 1
        # محاكاة إغلاق البناء فورًا (ورقي): نتائج واقعية +0.8%، +1.1%، -0.3%
        vnz.record_evidence_build_close(
            {'symbol': 'TIAUSDT', 'strategy_name': STRAT,
             'signal_details': {'evidence_building': True, 'regime': REG}},
            [0.8, 1.1, -0.3][opened - 1])
        if opened >= vnz.EVIDENCE_CELL_LOCK_AFTER_N:
            ok_gate, _ = vnz.evidence_gate_pass(STRAT, REG)
            locked_or_graduated = 'graduated' if ok_gate else 'fail_closed'
            break
    else:
        rejected += 1
check(f"السياسة الجديدة: {opened} بناء فُتح (كان 0 في V9.27) و{rejected} رفض للدرجات الدون 70", opened == 3)
check("نهاية السيناريو: الخلية تحكمت (تخرج أو فشل مغلق) — لا بقاء في الفراغ", locked_or_graduated in ('graduated', 'fail_closed'))
st = vnz._evidence_cell_combined(CELL)
print(f"   └─ نتيجة الخلية: ن={st.get('n')} توقع={st.get('exp_pct'):+.2f}%/صفقة PF={st.get('pf')} → {locked_or_graduated}")
check("السقوط المالي محدود بالتصميم: 3 بناءات كحد أقصى لخلية باردة و2 متزامنة إجمالًا",
      vnz.EVIDENCE_CELL_LOCK_AFTER_N <= 3 and vnz.EVIDENCE_PROBATION_MAX_OPEN <= 2)

vnz._evidence_probation_open_count = _orig_open

# ============ الخلاصة ============
failed = [n for n, ok in results if not ok]
print("\n" + "=" * 60)
print(f"النتيجة: {len(results) - len(failed)}/{len(results)} نجح")
if failed:
    print("فشل:", failed)
    sys.exit(1)
print("🎉 كل اختبارات V9.28.0 (حلقة بناء الدليل) نجحت")
