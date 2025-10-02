# app.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import hashlib
import calendar

# ===============================
# 구글시트 연동 (Secrets 우선)
# ===============================
SHEET_NAME_DATA = "시트1"
SHEET_NAME_TARGET = "시트2"

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]

def _build_client():
    if "gcp_service_account" not in st.secrets:
        st.error("Secrets에 gcp_service_account가 없습니다.")
        st.stop()
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        st.secrets["gcp_service_account"], scope
    )
    return gspread.authorize(creds)

client = _build_client()
SHEET_ID = st.secrets.get("SHEET_ID", "1y1rEG5iPGRiLo2GUzW4YrcWsv6dHChPBQxH033-9pts")

# ===============================
# 데이터
# ===============================
ws_data = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME_DATA)
ws_target = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME_TARGET)

df_data = pd.DataFrame(ws_data.get_all_records())
df_target = pd.DataFrame(ws_target.get_all_records())
df_data["날짜"] = pd.to_datetime(df_data["날짜"], errors="coerce")

# ===============================
# 거래처/분야 구성
#  - 요청: '밀리의서재', '크레마클럽' 추가
#  - '전체 매출' 순서: 영풍 다음에 두 구독사
#  - '구독' 탭 신설
# ===============================
base_vendors = [
    "PG사", "예스24", "교보문고", "알라딘", "영풍",
    "밀리의서재", "크레마클럽"                    # <<< 추가
]
subscription_vendors = ["밀리의서재", "크레마클럽"]  # <<< 구독 그룹

vendor_groups = {
    "전체 매출": base_vendors,
    "리커버": ["교보 리커버", "예스 리커버", "알라딘 리커버", "영풍(리커버)"],
    "전자책": ["예스(전자책)", "알라딘(전자)"],
    "구독": subscription_vendors,                 # <<< 신설 탭
}

# ===============================
# 유틸
# ===============================
def clean_numeric(df, cols):
    safe = [c for c in cols if c in df.columns]
    if not safe:
        # 안전한 컬럼이 없을 때 빈 DF 반환
        return pd.DataFrame(index=df.index)
    out = df[safe].apply(
        lambda x: pd.to_numeric(
            x.astype(str).str.replace(",", "").str.strip().replace("", "0"),
            errors="coerce"
        ),
        axis=0
    ).fillna(0)
    return out

def sum_for(df, vendors):
    """요청: 컬럼이 없을 때도 0 처리(테이블에 행을 유지하기 위함)."""
    nums = clean_numeric(df, vendors)
    result = {}
    for v in vendors:
        result[v] = float(nums[v].sum()) if (not nums.empty and v in nums.columns) else 0.0
    return result

def highlight_total(row):
    return ['font-weight: bold' if row['거래처'] == '합계' else '' for _ in row]

def calc_target_sum(vendors, start_date, end_date):
    months = pd.period_range(start=start_date.strftime("%Y-%m"),
                             end=end_date.strftime("%Y-%m"), freq="M")
    target_sum = {v: 0 for v in vendors}
    if "거래처" not in df_target.columns:
        return target_sum
    for month in months:
        month_key = f"{month.year}-{month.month:02d}"
        if month_key in df_target.columns:
            for v in vendors:
                if v in df_target["거래처"].values:
                    val = df_target.loc[df_target["거래처"] == v, month_key].values[0]
                    target_sum[v] += int(str(val).replace(",", "")) if val else 0
                else:
                    # 요청: 타겟 시트에 행이 없으면 0
                    target_sum[v] += 0
    return target_sum

def target_sum_for_months(vendors, year, month_list):
    total = 0
    if "거래처" not in df_target.columns:
        return 0
    for m in month_list:
        key = f"{year}-{m:02d}"
        if key in df_target.columns:
            for v in vendors:
                if v in df_target["거래처"].values:
                    val = df_target.loc[df_target["거래처"] == v, key].values[0]
                    total += int(str(val).replace(",", "")) if val else 0
                else:
                    total += 0
    return total

def month_name_kor(m): return f"{m}월"

def unique_key(*parts):
    raw = "||".join(map(str, parts))
    return "k_" + hashlib.md5(raw.encode()).hexdigest()[:12]

def last_day_of_month(y, m): return calendar.monthrange(y, m)[1]
def quarter_of_date(d: date): return (d.month - 1) // 3 + 1
def quarter_months(q: int): s = 3*(q-1)+1; return [s, s+1, s+2]

# ===============================
# 페이지 & 스타일
# ===============================
st.set_page_config(page_title="부크크 매출 현황", layout="wide")
st.markdown("""
<style>
:root{ --gap: 22px; }
.block-container{
  max-width: 1360px;
  padding: 0 1.5rem;
  padding-top: 1.9rem;    /* 타이틀 잘림 방지 */
}
.section-gap{height: var(--gap);}
.small-muted{color:#6b7280;font-size:12px}
.card{border:1px solid #e5e7eb;border-radius:12px;padding:14px;text-align:center;background:#fff}
.card h4{margin:0 0 6px 0;font-size:14px;color:#111827}
.card .value{font-size:20px;font-weight:700}
.card-primary{border:1px solid #c7d2fe;background:linear-gradient(180deg,#eef2ff,#fff)}
.card-accent{border:1px solid #bbf7d0;background:#f0fdf4}
.kpi-bar{display:flex;gap:12px;justify-content:space-between;margin:8px 0 0 0}
.kpi-pill{flex:1;border:1px solid #e5e7eb;border-radius:10px;padding:10px 12px;background:#fafafa;text-align:center}
.kpi-pill .label{font-size:12px;color:#6b7280;margin-bottom:4px}
.kpi-pill .num{font-size:16px;font-weight:700}
.footer-cards{display:flex;gap:12px;margin-top:10px}
.footer-card{flex:1;border:1px dashed #d1d5db;border-radius:10px;padding:12px;background:#fcfcff}
.footer-card .title{font-size:12px;color:#6b7280}
.footer-card .val{font-size:18px;font-weight:700}
h2{margin-top: var(--gap);}
.delta{font-weight:800;font-size:16px}
.delta-pos{color:#ef4444}  /* + : 빨강 */
.delta-neg{color:#2563eb}  /* - : 파랑 */
.main-title{
  text-align:center;
  margin: 18px 0 12px;
  padding-top: 6px;
}
.kpi-table-gap{height: 20px;}  /* 카드-표 사이 간격 넓힘 */
</style>
""", unsafe_allow_html=True)

st.markdown("<h1 class='main-title'>📊 부크크 매출 현황</h1>", unsafe_allow_html=True)
st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

# ===============================
# 기간 선택 (안정화 + 버튼 콜백: 어제 기준)
# ===============================
today = date.today()
yesterday = today - timedelta(days=1)
default_start = date(yesterday.year, yesterday.month, 1)
default_end = yesterday

if "start_picker" not in st.session_state: st.session_state.start_picker = default_start
if "end_picker"   not in st.session_state: st.session_state.end_picker   = default_end
if "applied_start" not in st.session_state: st.session_state.applied_start = default_start
if "applied_end"   not in st.session_state: st.session_state.applied_end   = default_end

def _apply_period(start: date, end: date):
    st.session_state.start_picker  = start
    st.session_state.end_picker    = end
    st.session_state.applied_start = start
    st.session_state.applied_end   = end

def cb_recent_month():
    base = yesterday
    _apply_period(date(base.year, base.month, 1), base)

def cb_recent_quarter():
    base = yesterday
    q = quarter_of_date(base)
    qms = quarter_months(q)
    _apply_period(date(base.year, qms[0], 1), base)

def cb_recent_year():
    base = yesterday
    _apply_period(date(base.year, 1, 1), base)

st.subheader("📅 기간 선택")
left, right = st.columns([4,1])

with left:
    with st.form("period_form", clear_on_submit=False):
        c1, c2, c3 = st.columns([1.1, 1.1, 0.9])
        c1.date_input("시작일", key="start_picker", format="YYYY-MM-DD")
        c2.date_input("종료일", key="end_picker", format="YYYY-MM-DD")
        if c3.form_submit_button("조회하기"):
            st.session_state.applied_start = st.session_state.start_picker
            st.session_state.applied_end   = st.session_state.end_picker

with right:
    st.markdown("<div style='height:2px'></div>", unsafe_allow_html=True)
    st.button("최근 한달",  use_container_width=True, on_click=cb_recent_month,   key="btn_m")
    st.button("최근 분기",  use_container_width=True, on_click=cb_recent_quarter, key="btn_q")
    st.button("최근 1년",  use_container_width=True, on_click=cb_recent_year,    key="btn_y")

start_date = st.session_state.applied_start
end_date   = st.session_state.applied_end
st.caption(f"적용된 기간: {pd.to_datetime(start_date).strftime('%Y년 %m월 %d일')} ~ {pd.to_datetime(end_date).strftime('%Y년 %m월 %d일')}")
st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

# ===============================
# 필터링
# ===============================
df_period = df_data[(df_data["날짜"] >= pd.to_datetime(start_date)) &
                    (df_data["날짜"] <= pd.to_datetime(end_date))].copy()

# ===============================
# 상단 KPI 카드 공통 렌더러
# ===============================
def render_top_cards(total_target, total_prev, total_actual):
    ach = f"{(total_actual/total_target*100):.1f}%" if total_target>0 else "-"
    yoy = f"{((total_actual-total_prev)/total_prev*100):.1f}%" if total_prev>0 else "-"
    c1,c2,c3,c4,c5 = st.columns(5)
    with c1:
        st.markdown(f"<div class='card'><h4>목표 매출</h4><div class='value'>{total_target:,.0f} 원</div></div>", unsafe_allow_html=True)
    with c2:
        st.markdown(f"<div class='card'><h4>전년 매출</h4><div class='value'>{total_prev:,.0f} 원</div></div>", unsafe_allow_html=True)   # ← 변경
    with c3:
        st.markdown(f"<div class='card card-primary'><h4>실제 매출</h4><div class='value'>{total_actual:,.0f} 원</div></div>", unsafe_allow_html=True) # ← 변경
    with c4:
        st.markdown(f"<div class='card'><h4>달성률</h4><div class='value'>{ach}</div></div>", unsafe_allow_html=True)
    with c5:
        st.markdown(f"<div class='card'><h4>YoY</h4><div class='value'>{yoy}</div></div>", unsafe_allow_html=True)


# ===============================
# 탭
# ===============================
tabs = st.tabs(list(vendor_groups.keys()))

for tab_name, tab in zip(vendor_groups.keys(), tabs):
    with tab:
        st.subheader(f"📊 {tab_name}")

        vendors = vendor_groups[tab_name]

        # 실제(기간) / 전년 동기간(기간) — 컬럼이 없어도 0 처리
        sdt = pd.to_datetime(start_date)
        edt = pd.to_datetime(end_date)

        actual_sum = sum_for(df_period, vendors)

        ly_s = sdt.replace(year=sdt.year-1); ly_e = edt.replace(year=edt.year-1)
        df_ly = df_data[(df_data["날짜"] >= ly_s) & (df_data["날짜"] <= ly_e)]
        prev_sum = sum_for(df_ly, vendors)

        # 목표(기간 월 합)
        target_sum = calc_target_sum(vendors, sdt, edt)

        # 상단 KPI 카드 (탭별)
        T = sum(target_sum.values()); P = sum(prev_sum.values()); A = sum(actual_sum.values())
        render_top_cards(T, P, A)

        # 카드-표 간격
        st.markdown("<div class='kpi-table-gap'></div>", unsafe_allow_html=True)

        # 표(요청: 컬럼 없어도 행을 보이게, 모두 vendors 기준으로 생성)
        rows = []
        for v in vendors:
            a = actual_sum.get(v, 0); p = prev_sum.get(v, 0); t = target_sum.get(v, 0)
            achieve = f"{(a/t*100):.1f}%" if t>0 else "-"
            yoy     = f"{((a-p)/p*100):.1f}%" if p>0 else "-"
            rows.append({"거래처":v,"목표 매출":f"{t:,.0f} 원","전년 매출":f"{p:,.0f} 원","실제 매출":f"{a:,.0f} 원","달성률":achieve,"YoY":yoy})

        rows.append({"거래처":"합계","목표 매출":f"{T:,.0f} 원","전년 매출":f"{P:,.0f} 원","실제 매출":f"{A:,.0f} 원",
                     "달성률":f"{(A/T*100):.1f}%" if T>0 else "-",
                     "YoY":f"{((A-P)/P*100):.1f}%" if P>0 else "-"})
        st.dataframe(
            pd.DataFrame(rows).style
                .set_properties(**{"text-align":"right"}, subset=["목표 매출","전년 매출","실제 매출"])
                .apply(highlight_total, axis=1),
            use_container_width=True
        )

        # 증감액
        st.markdown(f"""
<div class="footer-cards">
  <div class="footer-card"><div class="title">전년 대비 증가액</div><div class="val">{A-P:+,.0f} 원</div></div>  
  <div class="footer-card"><div class="title">목표 대비 증가액</div><div class="val">{A-T:+,.0f} 원</div></div>  
</div>
""", unsafe_allow_html=True)


        # =======================
        # 🎯 목표 달성율 (어제 기준)
        # =======================
        st.markdown("### 🎯 목표 달성율")

        base = yesterday
        vendors_all = base_vendors  # 연/분기/월 총합은 전체 거래처 기준

        y_start = date(base.year, 1, 1)
        q = quarter_of_date(base); qms = quarter_months(q); q_start = date(base.year, qms[0], 1)
        m_start = date(base.year, base.month, 1)

        def actual_sum_in_range(d1: date, d2: date):
            if d2 < d1: return 0
            m = (df_data["날짜"] >= pd.to_datetime(d1)) & (df_data["날짜"] <= pd.to_datetime(d2))
            return clean_numeric(df_data.loc[m], [c for c in vendors_all if c in df_data.columns])\
                        .sum(axis=1).sum()

        actual_y = actual_sum_in_range(y_start, base)
        actual_q = actual_sum_in_range(q_start, base)
        actual_m = actual_sum_in_range(m_start, base)

        target_y = target_sum_for_months(vendors_all, base.year, list(range(1,13)))
        target_q = target_sum_for_months(vendors_all, base.year, qms)
        target_m = target_sum_for_months(vendors_all, base.year, [base.month])

        y_days = (date(base.year+1,1,1) - y_start).days
        y_elapsed = (base - y_start).days + 1
        y_pct = y_elapsed / y_days * 100

        q_end = date(base.year, qms[-1], last_day_of_month(base.year, qms[-1]))
        q_days = (q_end - q_start).days + 1
        q_elapsed = (min(base, q_end) - q_start).days + 1
        q_pct = q_elapsed / q_days * 100

        m_end = date(base.year, base.month, last_day_of_month(base.year, base.month))
        m_days = (m_end - m_start).days + 1
        m_elapsed = (min(base, m_end) - m_start).days + 1
        m_pct = m_elapsed / m_days * 100

        def donut(title, actual, target, key_tag, scope):
            ratio = (actual/target) if target>0 else 0.0
            filled = min(max(ratio,0),1.0)
            fig = go.Figure(data=[go.Pie(values=[filled, 1-filled], hole=0.72,
                                         sort=False, direction="clockwise",
                                         textinfo="none", hoverinfo="skip",
                                         marker=dict(colors=["#3b82f6" if ratio<1 else "#22c55e", "#e5e7eb"]))])
            fig.update_layout(
                title=dict(text=title, x=0.5, y=0.93),
                annotations=[dict(text=f"{ratio*100:.1f}%", x=0.5,y=0.5,showarrow=False,font=dict(size=22,color="#111827")),
                             dict(text="to Goal", x=0.5,y=0.40,showarrow=False,font=dict(size=12,color="#6b7280"))],
                showlegend=False, margin=dict(l=10,r=10,t=40,b=10), height=260
            )
            st.plotly_chart(fig, use_container_width=True,
                            key=unique_key("goal", scope, key_tag, title))
            delta = target - actual
            cls = "delta-pos" if delta > 0 else ("delta-neg" if delta < 0 else "")
            st.markdown(
                f"<div class='small-muted'>목표 {target:,.0f} 원 · 실제 {actual:,.0f} 원<br>"
                f"<span class='delta {cls}'>차액(목표-실제) {abs(delta):,.0f} 원</span></div>",
                unsafe_allow_html=True
            )
            return ratio*100

        c1,c2,c3 = st.columns(3)
        with c1: y_ratio = donut("연도 달성율", actual_y, target_y, "y", tab_name)
        with c2: q_ratio = donut("분기 달성율", actual_q, target_q, "q", tab_name)
        with c3: m_ratio = donut("월 달성율",   actual_m, target_m, "m", tab_name)

        st.markdown(f"""
        <div class="kpi-bar" style="margin-top:8px;">
          <div class="kpi-pill">
            <div class="label">연도 경과율(어제)</div>
            <div class="num">{y_pct:.1f}%</div>
            <div class="small-muted">달성-경과: {(y_ratio - y_pct):+.1f}%p</div>
          </div>
          <div class="kpi-pill">
            <div class="label">분기 경과율(어제)</div>
            <div class="num">{q_pct:.1f}%</div>
            <div class="small-muted">달성-경과: {(q_ratio - q_pct):+.1f}%p</div>
          </div>
          <div class="kpi-pill">
            <div class="label">월 경과율(어제)</div>
            <div class="num">{m_pct:.1f}%</div>
            <div class="small-muted">달성-경과: {(m_ratio - m_pct):+.1f}%p</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

        # =======================
        # 일일 매출 추이 (동요일 보정)
        # =======================
        st.markdown("### 📈 일일 매출 추이 (올해 vs 작년, 동요일 기준)")
        safe_cols = [c for c in vendors if c in df_period.columns]
        daily_cur = df_period[["날짜"] + safe_cols].copy()
        daily_cur[safe_cols] = clean_numeric(daily_cur, safe_cols)
        daily_cur["합계"] = daily_cur[safe_cols].sum(axis=1)

        wd_diff = (sdt.weekday() - sdt.replace(year=sdt.year - 1).weekday())
        ly_start_g = sdt.replace(year=sdt.year - 1) + timedelta(days=wd_diff)
        ly_end_g   = ly_start_g + (edt - sdt)
        df_ly_g = df_data[(df_data["날짜"] >= ly_start_g) & (df_data["날짜"] <= ly_end_g)].copy()

        daily_ly = df_ly_g[["날짜"] + safe_cols].copy()
        daily_ly[safe_cols] = clean_numeric(daily_ly, safe_cols)
        daily_ly["합계"] = daily_ly[safe_cols].sum(axis=1)

        n = min(len(daily_cur), len(daily_ly))
        df_chart = pd.DataFrame({
            "날짜": pd.concat([daily_cur["날짜"].iloc[:n].reset_index(drop=True),
                              daily_cur["날짜"].iloc[:n].reset_index(drop=True)], ignore_index=True),
            "매출": pd.concat([daily_cur["합계"].iloc[:n].reset_index(drop=True),
                              daily_ly["합계"].iloc[:n].reset_index(drop=True)], ignore_index=True),
            "구분": ["올해"]*n + ["작년(동요일 보정)"]*n
        })
        fig = px.line(df_chart, x="날짜", y="매출", color="구분",
                      labels={"날짜":"날짜","매출":"매출액(원)","구분":"구분"})
        fig.update_traces(hovertemplate="날짜=%{x|%Y-%m-%d}<br>매출=%{y:,.0f}원<extra></extra>")
        st.plotly_chart(fig, use_container_width=True, key=unique_key("daily", tab_name))

        st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

        # =======================
        # 월별 추이 (최근 3년, 확정된 월만)
        #  - 요청: '구독(분야)' 그래프 추가
        # =======================
        st.markdown("### 📈 거래처별 및 분야별 매출 추이 (월별, 최근 3개 연도)")
        df_all = df_data.copy()
        df_all["연"] = df_all["날짜"].dt.year
        df_all["월"] = df_all["날짜"].dt.month

        this_year = today.year
        yrs_all = sorted(int(y) for y in df_all["연"].dropna().unique())
        yrs_clip = [y for y in yrs_all if y <= this_year]
        years = yrs_clip[-3:] if len(yrs_clip) >= 3 else yrs_clip

        last_day_curr = last_day_of_month(yesterday.year, yesterday.month)
        confirmed_limit = yesterday.month if yesterday.day == last_day_curr else max(1, yesterday.month-1)

        def monthly_series(cols):
            cols = [c for c in cols if c in df_all.columns]
            if not cols:
                base_s = pd.Series([0.0]*12, index=range(1,13))
                return {y: base_s.copy() for y in years}
            nums = clean_numeric(df_all, cols)
            tmp = pd.concat([df_all[["연","월"]], nums], axis=1)
            tmp["합"] = nums.sum(axis=1)
            g = tmp.groupby(["연","월"], as_index=False)["합"].sum()
            out = {}
            for y in years:
                s = pd.Series(0.0, index=range(1,13))
                if y in g["연"].unique():
                    sub = g[g["연"]==y].set_index("월")["합"]
                    s.loc[sub.index] = sub.values
                if y == yesterday.year:
                    s.loc[range(confirmed_limit+1, 13)] = np.nan
                out[y] = s
            return out

        def render_small(title, series_dict, tname, idx):
            plot_df = pd.DataFrame({str(y): series_dict[y] for y in years}, index=range(1,13))
            plot_df.index = [month_name_kor(m) for m in plot_df.index]
            plot_df = plot_df.reset_index().rename(columns={"index":"월"})
            mdf = plot_df.melt(id_vars=["월"], var_name="연도", value_name="매출")
            fig = px.line(mdf, x="월", y="매출", color="연도", markers=True,
                          title=title,
                          category_orders={"월":[f"{m}월" for m in range(1,13)]},
                          labels={"월":"월","매출":"매출액(원)","연도":"연도"})
            fig.update_traces(hovertemplate="월=%{x}<br>매출=%{y:,.0f}원<extra></extra>")
            fig.update_layout(height=300, margin=dict(l=10,r=10,t=40,b=10))
            st.plotly_chart(fig, use_container_width=True,
                            key=unique_key("trend", tname, title, idx, "-".join(map(str,years))))

        # 요청: 거래처 개별 + 분야(전체/리커버/전자책/구독)
        vendor_panels = [("합계(거래처)", base_vendors)] + [(v,[v]) for v in base_vendors]
        field_panels  = [("전체매출(분야)", base_vendors),
                         ("리커버(분야)", vendor_groups["리커버"]),
                         ("전자책(분야)", vendor_groups["전자책"]),
                         ("구독(분야)", subscription_vendors)]  # <<< 추가
        panels = vendor_panels + field_panels

        for i in range(0, len(panels), 2):
            cols2 = st.columns(2)
            for j, (title, cols_list) in enumerate(panels[i:i+2]):
                with cols2[j]:
                    series = monthly_series(cols_list)
                    render_small(title, series, tab_name, i+j)
