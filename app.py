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
# 구글시트 연동 (Secrets 우선, 파일은 로컬 개발용 fallback)
# ===============================
SHEET_NAME_DATA = "시트1"
SHEET_NAME_TARGET = "시트2"

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]

def _build_client():
    if "gcp_service_account" not in st.secrets:
        # 배포 환경에서 secrets가 없다면 에러 안내 후 중단
        st.error("Secrets에 gcp_service_account가 없습니다. Streamlit Cloud의 Secrets에 서비스계정 JSON을 TOML 형식으로 붙여넣어 주세요.")
        st.stop()
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], scope)
    return gspread.authorize(creds)

client = _build_client()
SHEET_ID = st.secrets.get("SHEET_ID", "1y1rEG5iPGRiLo2GUzW4YrcWsv6dHChPBQxH033-9pts")

# ===============================
# 데이터 불러오기
# ===============================
ws_data = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME_DATA)
ws_target = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME_TARGET)

df_data = pd.DataFrame(ws_data.get_all_records())
df_target = pd.DataFrame(ws_target.get_all_records())

df_data["날짜"] = pd.to_datetime(df_data["날짜"], errors="coerce")

# -------------------------------
# 거래처/분야 구성
# -------------------------------
base_vendors = ["PG사", "예스24", "교보문고", "알라딘", "영풍"]
vendor_groups = {
    "전체 매출": base_vendors,
    "리커버": ["교보 리커버", "예스 리커버", "알라딘 리커버", "영풍(리커버)"],
    "전자책": ["예스(전자책)", "알라딘(전자)"]
}

# ===============================
# 공통 함수
# ===============================
def clean_numeric(df, cols):
    safe = [c for c in cols if c in df.columns]
    if not safe:
        return pd.DataFrame(index=df.index)
    out = df[safe].apply(
        lambda x: pd.to_numeric(
            x.astype(str).str.replace(",", "").str.strip().replace("", "0"),
            errors="coerce"
        ),
        axis=0
    ).fillna(0)
    return out

def highlight_total(row):
    return ['font-weight: bold' if row['거래처'] == '합계' else '' for _ in row]

def calc_target_sum(vendors, start_date, end_date):
    """선택된 기간의 연월별 목표 매출 합산 (YYYY-MM 컬럼 기준)"""
    months = pd.period_range(start=start_date.strftime("%Y-%m"),
                             end=end_date.strftime("%Y-%m"), freq="M")
    target_sum = {v: 0 for v in vendors}
    for month in months:
        month_key = f"{month.year}-{month.month:02d}"
        if month_key in df_target.columns and "거래처" in df_target.columns:
            for v in vendors:
                if v in df_target["거래처"].values:
                    val = df_target.loc[df_target["거래처"] == v, month_key].values[0]
                    target_sum[v] += int(str(val).replace(",", "")) if val else 0
    return target_sum

def target_sum_for_months(vendors, year, month_list):
    """특정 연도의 여러 월 목표 합계(연/분기/월 달성율 계산용)"""
    total = 0
    if "거래처" not in df_target.columns:
        return 0
    for m in month_list:
        month_key = f"{year}-{m:02d}"
        if month_key in df_target.columns:
            for v in vendors:
                if v in df_target["거래처"].values:
                    val = df_target.loc[df_target["거래처"] == v, month_key].values[0]
                    total += int(str(val).replace(",", "")) if val else 0
    return total

def month_name_kor(m: int) -> str:
    return f"{m}월"

def unique_key(*parts) -> str:
    """한글/기호 포함 안전한 고유 key 생성"""
    raw = "||".join(map(str, parts))
    return "k_" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]

def last_day_of_month(y, m):
    return calendar.monthrange(y, m)[1]

def quarter_of_date(d: date) -> int:
    return (d.month - 1) // 3 + 1

def quarter_months(q: int) -> list:
    start = 3*(q-1) + 1
    return [start, start+1, start+2]

# ===============================
# Streamlit UI (여백/스타일)
# ===============================
st.set_page_config(page_title="부크크 매출 현황", layout="wide")

st.markdown("""
<style>
:root{ --gap: 22px; }
.block-container{
    max-width: 1360px;
    padding-left: 1.5rem;
    padding-right: 1.5rem;
}
.section-gap{height: var(--gap);}
.small-muted{color:#6b7280;font-size:12px}
.card{border:1px solid #e5e7eb;border-radius:12px;padding:14px;text-align:center;background:#fff}
.card h4{margin:0 0 6px 0;font-size:14px;color:#111827}
.card .value{font-size:20px;font-weight:700}
.card-primary{border:1px solid #c7d2fe;background:linear-gradient(180deg,#eef2ff, #fff)}
.card-accent{border:1px solid #bbf7d0;background:#f0fdf4}
.kpi-bar{display:flex;gap:12px;justify-content:space-between;margin:8px 0 0 0}
.kpi-pill{flex:1;border:1px solid #e5e7eb;border-radius:10px;padding:10px 12px;background:#fafafa;text-align:center}
.kpi-pill .label{font-size:12px;color:#6b7280;margin-bottom:4px}
.kpi-pill .num{font-size:16px;font-weight:700}
.footer-cards{display:flex;gap:12px;margin-top:10px}
.footer-card{flex:1;border:1px dashed #d1d5db;border-radius:10px;padding:12px;background:#fcfcff}
.footer-card .title{font-size:12px;color:#6b7280}
.footer-card .val{font-size:18px;font-weight:700}
.hstack{display:flex;gap:12px;align-items:end}
h2{margin-top: var(--gap);}
</style>
""", unsafe_allow_html=True)

# 가운데 정렬 타이틀
st.markdown("<h1 style='text-align:center;margin-top:4px;'>📊 부크크 매출 현황</h1>", unsafe_allow_html=True)
st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

# -------------------------------
# 기간 선택
#  - 기본값: 종료일=어제, 시작일=해당 월 1일
#  - 버튼: 최근 한달 / 최근 분기 / 최근 1년 (즉시 적용)
# -------------------------------
today = date.today()
yesterday = today - timedelta(days=1)
default_start = date(yesterday.year, yesterday.month, 1)
default_end = yesterday

if "selected_start" not in st.session_state:
    st.session_state.selected_start = default_start
if "selected_end" not in st.session_state:
    st.session_state.selected_end = default_end
if "applied_start" not in st.session_state:
    st.session_state.applied_start = default_start
if "applied_end" not in st.session_state:
    st.session_state.applied_end = default_end

st.subheader("📅 기간 선택")

left, right = st.columns([4,1])
with left:
    with st.form("period_form"):
        f1, f2, f3 = st.columns([1.1, 1.1, 0.9])
        s_start = f1.date_input("시작일", st.session_state.selected_start, format="YYYY-MM-DD")
        s_end   = f2.date_input("종료일", st.session_state.selected_end, format="YYYY-MM-DD")
        submitted = f3.form_submit_button("조회하기")
    if submitted:
        st.session_state.selected_start = s_start
        st.session_state.selected_end = s_end
        st.session_state.applied_start = s_start
        st.session_state.applied_end = s_end

with right:
    st.markdown("<div style='height:26px'></div>", unsafe_allow_html=True)

    # 최근 한달: 실행 월 1일 ~ 어제
    if st.button("최근 한달", help="실행 월 1일 ~ 어제", key="btn-month-now"):
        start = date(yesterday.year, yesterday.month, 1)
        end = yesterday
        st.session_state.selected_start = start
        st.session_state.selected_end = end
        st.session_state.applied_start = start
        st.session_state.applied_end = end

    # 최근 분기: 오늘이 속한 분기의 첫날 ~ 어제
    if st.button("최근 분기", help="해당 연도 현재 분기 시작일 ~ 어제", key="btn-quarter-now"):
        q = quarter_of_date(today)
        qms = quarter_months(q)
        start = date(today.year, qms[0], 1)
        end = yesterday
        st.session_state.selected_start = start
        st.session_state.selected_end = end
        st.session_state.applied_start = start
        st.session_state.applied_end = end

    # 최근 1년(연초부터): 해당 연도 1월 1일 ~ 어제
    if st.button("최근 1년", help="해당 연도 1월 1일 ~ 어제", key="btn-year-now"):
        start = date(today.year, 1, 1)
        end = yesterday
        st.session_state.selected_start = start
        st.session_state.selected_end = end
        st.session_state.applied_start = start
        st.session_state.applied_end = end

# 이후 로직은 '적용된 기간'만 사용
start_date = st.session_state.applied_start
end_date = st.session_state.applied_end
st.caption(f"적용된 기간: {pd.to_datetime(start_date).strftime('%Y년 %m월 %d일')} ~ {pd.to_datetime(end_date).strftime('%Y년 %m월 %d일')}")
st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

# ===============================
# 선택 기간 필터
# ===============================
df_period = df_data[(df_data["날짜"] >= pd.to_datetime(start_date)) &
                    (df_data["날짜"] <= pd.to_datetime(end_date))].copy()

# ===============================
# 탭 (전체/리커버/전자책)
# ===============================
tabs = st.tabs(list(vendor_groups.keys()))

for tab_name, tab in zip(vendor_groups.keys(), tabs):
    with tab:
        st.subheader(f"📊 {tab_name}")

        vendors = vendor_groups[tab_name]
        available_vendors = [v for v in vendors if v in df_period.columns]

        # 실제 매출(선택기간)
        df_selected = clean_numeric(df_period, available_vendors)
        actual_sum = df_selected.sum().to_dict()

        # 전년도 동일 날짜(테이블/요약용)
        start_dt = pd.to_datetime(start_date)
        end_dt   = pd.to_datetime(end_date)
        last_year_start = start_dt.replace(year=start_dt.year - 1)
        last_year_end   = end_dt.replace(year=end_dt.year - 1)
        df_last_year = df_data[(df_data["날짜"] >= last_year_start) & (df_data["날짜"] <= last_year_end)].copy()
        df_last_selected = clean_numeric(df_last_year, available_vendors)
        last_sum = df_last_selected.sum().to_dict()

        # 목표 매출(선택기간 월 합산)
        target_sum = calc_target_sum(available_vendors, start_dt, end_dt)

        # ---------------------------
        # 거래처별 테이블 데이터
        # ---------------------------
        rows = []
        for v in available_vendors:
            actual = actual_sum.get(v, 0)
            prev   = last_sum.get(v, 0)
            target = target_sum.get(v, 0)
            achieve = f"{(actual/target*100):.1f}%" if target > 0 else "-"
            yoy     = f"{((actual-prev)/prev*100):.1f}%" if prev > 0 else "-"
            rows.append({
                "거래처": v,
                "목표 매출": f"{target:,} 원",
                "전년 매출": f"{prev:,} 원",
                "실제 매출": f"{actual:,} 원",
                "달성률": achieve,
                "YoY": yoy
            })

        total_target = sum(target_sum.values())
        total_prev   = sum(last_sum.values())
        total_actual = sum(actual_sum.values())
        total_achieve = f"{(total_actual/total_target*100):.1f}%" if total_target > 0 else "-"
        total_yoy     = f"{((total_actual-total_prev)/total_prev*100):.1f}%" if total_prev > 0 else "-"

        rows.append({
            "거래처": "합계",
            "목표 매출": f"{total_target:,} 원",
            "전년 매출": f"{total_prev:,} 원",
            "실제 매출": f"{total_actual:,} 원",
            "달성률": total_achieve,
            "YoY": total_yoy
        })
        df_display = pd.DataFrame(rows)

        # ===============================
        # 상단 카드 요약 (모든 탭에 적용)
        # ===============================
        k1, k2, k3, k4, k5 = st.columns(5)
        def card(title, value, klass=""):
            st.markdown(f"""
            <div class="card {klass}">
              <h4>{title}</h4>
              <div class="value">{value}</div>
            </div>
            """, unsafe_allow_html=True)

        with k1: card("목표 매출", f"{total_target:,}원")
        with k2: card("전년 매출", f"{total_prev:,}원")
        with k3: card("실제 매출", f"{total_actual:,}원", "card-primary")
        with k4: card("달성률", total_achieve, "card-accent")
        with k5: card("YoY", total_yoy)

        st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

        # ===============================
        # 거래처별 매출 테이블 (합계 bold)
        # ===============================
        styled_table = (
            df_display.style
            .set_properties(**{"text-align": "right"}, subset=["목표 매출", "전년 매출", "실제 매출"])
            .apply(highlight_total, axis=1)
        )
        st.dataframe(styled_table, use_container_width=True)

        # ➤ 증감액 카드 (합계 기준)
        diff_prev   = total_actual - total_prev
        diff_target = total_actual - total_target
        st.markdown(f"""
        <div class="footer-cards">
          <div class="footer-card">
            <div class="title">전년 대비 증가액</div>
            <div class="val">{diff_prev:+,} 원</div>
          </div>
          <div class="footer-card">
            <div class="title">목표 대비 증가액</div>
            <div class="val">{diff_target:+,} 원</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

        # =========================================================
        #  🎯 목표 달성율 (오늘 기준)
        # =========================================================
        st.markdown("### 🎯 목표 달성율 (오늘 기준)")

        vendors_all = base_vendors[:]  # 전체 매출 기준

        # 어제 기준으로 클램프
        today_d = date.today()
        yday = today_d - timedelta(days=1)

        def clamp_end(start_d: date, end_d: date):
            return end_d if end_d >= start_d else start_d - timedelta(days=1)

        # YTD/QTD/MTD 기간
        ytd_start = date(today_d.year, 1, 1)
        qtd_q = quarter_of_date(today_d)
        qtd_months = quarter_months(qtd_q)
        qtd_start = date(today_d.year, qtd_months[0], 1)
        mtd_start = date(today_d.year, today_d.month, 1)

        ytd_end = clamp_end(ytd_start, yday)
        qtd_end = clamp_end(qtd_start, yday)
        mtd_end = clamp_end(mtd_start, yday)

        # 실제 매출 합계
        def actual_sum_in_range(d1: date, d2: date):
            if d2 < d1:
                return 0
            mask = (df_data["날짜"] >= pd.to_datetime(d1)) & (df_data["날짜"] <= pd.to_datetime(d2))
            df_sub = df_data.loc[mask]
            nums = clean_numeric(df_sub, vendors_all)
            return nums.sum(axis=1).sum()

        actual_ytd = actual_sum_in_range(ytd_start, ytd_end)
        actual_qtd = actual_sum_in_range(qtd_start, qtd_end)
        actual_mtd = actual_sum_in_range(mtd_start, mtd_end)

        # 목표 합계 (연/분기/월)
        target_ytd = target_sum_for_months(vendors_all, today_d.year, list(range(1,13)))
        target_qtd = target_sum_for_months(vendors_all, today_d.year, qtd_months)
        target_mtd = target_sum_for_months(vendors_all, today_d.year, [today_d.month])

        # 도넛 차트
        def donut(title, actual, target, key_tag):
            ratio = (actual / target) if target > 0 else 0.0
            filled = min(max(ratio, 0), 1.0)
            remaining = 1 - filled
            fig = go.Figure(data=[go.Pie(
                values=[filled, remaining],
                hole=0.72,
                sort=False,
                direction="clockwise",
                textinfo="none",
                hoverinfo="skip",
                marker=dict(colors=["#22c55e", "#e5e7eb"]) if ratio>=1 else dict(colors=["#3b82f6", "#e5e7eb"])
            )])
            fig.update_layout(
                title=dict(text=title, x=0.5, y=0.93),
                annotations=[dict(text=f"{ratio*100:.1f}%", x=0.5, y=0.5, showarrow=False, font=dict(size=22, color="#111827")),
                             dict(text="to Goal", x=0.5, y=0.40, showarrow=False, font=dict(size=12, color="#6b7280"))],
                showlegend=False,
                margin=dict(l=10,r=10,t=40,b=10),
                height=260
            )
            st.plotly_chart(fig, use_container_width=True, key=unique_key("goal", key_tag, title))
            st.caption(f"목표 {target:,.0f} 원 · 실제 {actual:,.0f} 원")

        gc1, gc2, gc3 = st.columns(3)
        with gc1: donut("연도 달성율", actual_ytd, target_ytd, f"{tab_name}-year")
        with gc2: donut("분기 달성율", actual_qtd, target_qtd, f"{tab_name}-quarter")
        with gc3: donut("월 달성율",   actual_mtd, target_mtd, f"{tab_name}-month")

        # ➜ 경과율(어제 기준)
        ref = yday
        # 연도
        y_start = date(today_d.year, 1, 1)
        days_elapsed = max((ref - y_start).days + 1, 0)
        days_in_year = (date(today_d.year + 1, 1, 1) - y_start).days
        year_pct = (days_elapsed / days_in_year * 100) if days_in_year else 0
        # 분기
        q = quarter_of_date(today_d)
        qm = quarter_months(q)
        q_start = date(today_d.year, qm[0], 1)
        q_end   = date(today_d.year, qm[-1], last_day_of_month(today_d.year, qm[-1]))
        q_elapsed = max(min((ref - q_start).days + 1, (q_end - q_start).days + 1), 0)
        q_days    = (q_end - q_start).days + 1
        q_pct = (q_elapsed / q_days * 100) if q_days else 0
        # 월
        m_start = date(today_d.year, today_d.month, 1)
        m_end   = date(today_d.year, today_d.month, last_day_of_month(today_d.year, today_d.month))
        m_elapsed = max(min((ref - m_start).days + 1, (m_end - m_start).days + 1), 0)
        m_days    = (m_end - m_start).days + 1
        m_pct = (m_elapsed / m_days * 100) if m_days else 0

        st.markdown(f"""
        <div class="kpi-bar" style="margin-top:8px;">
          <div class="kpi-pill">
            <div class="label">연도 경과율(어제)</div>
            <div class="num">{year_pct:.1f}%</div>
            <div class="small-muted">{days_elapsed}/{days_in_year}</div>
          </div>
          <div class="kpi-pill">
            <div class="label">분기 경과율(어제)</div>
            <div class="num">{q_pct:.1f}%</div>
            <div class="small-muted">{q_elapsed}/{q_days}</div>
          </div>
          <div class="kpi-pill">
            <div class="label">월 경과율(어제)</div>
            <div class="num">{m_pct:.1f}%</div>
            <div class="small-muted">{m_elapsed}/{m_days}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

        # ===============================
        # 일일 매출 추이 (올해 vs 작년, 동요일 보정)
        # ===============================
        st.markdown("### 📈 일일 매출 추이 (올해 vs 작년, 동요일 기준)")
        daily_current = df_period[["날짜"] + available_vendors].copy()
        daily_current[available_vendors] = clean_numeric(daily_current, available_vendors)
        daily_current["합계"] = daily_current[available_vendors].sum(axis=1)

        weekday_diff = (start_dt.weekday() - start_dt.replace(year=start_dt.year - 1).weekday())
        last_year_start_graph = start_dt.replace(year=start_dt.year - 1) + timedelta(days=weekday_diff)
        last_year_end_graph   = last_year_start_graph + (end_dt - start_dt)
        df_last_year_graph = df_data[(df_data["날짜"] >= last_year_start_graph) &
                                     (df_data["날짜"] <= last_year_end_graph)].copy()

        daily_last = df_last_year_graph[["날짜"] + available_vendors].copy()
        daily_last[available_vendors] = clean_numeric(daily_last, available_vendors)
        daily_last["합계"] = daily_last[available_vendors].sum(axis=1)

        n = min(len(daily_current), len(daily_last))
        df_chart = pd.DataFrame({
            "날짜": pd.concat(
                [daily_current["날짜"].iloc[:n].reset_index(drop=True),
                 daily_current["날짜"].iloc[:n].reset_index(drop=True)],
                ignore_index=True
            ),
            "매출": pd.concat(
                [daily_current["합계"].iloc[:n].reset_index(drop=True),
                 daily_last["합계"].iloc[:n].reset_index(drop=True)],
                ignore_index=True
            ),
            "구분": ["올해"] * n + ["작년(동요일 보정)"] * n
        })
        fig_daily = px.line(df_chart, x="날짜", y="매출", color="구분",
                            labels={"날짜": "날짜", "매출": "매출액(원)", "구분": "구분"})
        fig_daily.update_traces(hovertemplate="날짜=%{x|%Y-%m-%d}<br>매출=%{y:,.0f}원<extra></extra>")
        st.plotly_chart(fig_daily, use_container_width=True, key=unique_key("daily", tab_name))

        st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)

        # =========================================================
        #  거래처별 및 분야별 매출 추이 (월별, 최근 3개 연도)
        # =========================================================
        st.markdown("### 📈 거래처별 및 분야별 매출 추이 (월별, 최근 3개 연도)")

        df_all = df_data.copy()
        df_all["연"] = df_all["날짜"].dt.year
        df_all["월"] = df_all["날짜"].dt.month

        this_year = date.today().year
        year_list_all = sorted(int(y) for y in df_all["연"].dropna().unique())
        year_list_clip = [y for y in year_list_all if y <= this_year]  # 미래연도 제외
        years = year_list_clip[-3:] if len(year_list_clip) >= 3 else year_list_clip

        def monthly_series_for_cols(cols):
            cols = [c for c in cols if c in df_all.columns]
            if not cols:
                base = pd.Series([0.0]*12, index=range(1,13))
                return {y: base.copy() for y in years}
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
                if y == this_year and date.today().month < 12:
                    s.loc[range(date.today().month+1,13)] = np.nan
                out[y] = s
            return out

        def render_small_line(title, series_dict, tname, idx):
            plot_df = pd.DataFrame({str(y): series_dict[y] for y in years}, index=range(1,13))
            plot_df.index = [month_name_kor(m) for m in plot_df.index]
            plot_df = plot_df.reset_index().rename(columns={"index":"월"})
            mdf = plot_df.melt(id_vars=["월"], var_name="연도", value_name="매출")
            fig = px.line(mdf, x="월", y="매출", color="연도", markers=True,
                          title=title,
                          category_orders={"월":[f"{m}월" for m in range(1,13)]},
                          labels={"월":"월", "매출":"매출액(원)", "연도":"연도"})
            fig.update_traces(hovertemplate="월=%{x}<br>매출=%{y:,.0f}원<extra></extra>")
            fig.update_layout(height=300, margin=dict(l=10,r=10,t=40,b=10))
            st.plotly_chart(fig, use_container_width=True,
                            key=unique_key("trend", tname, title, idx, "-".join(map(str,years))))

        vendor_panels = [("합계(거래처)", base_vendors)] + [(v,[v]) for v in base_vendors]
        field_panels  = [("전체매출(분야)", base_vendors),
                         ("리커버(분야)", vendor_groups["리커버"]),
                         ("전자책(분야)", vendor_groups["전자책"])]
        panels = vendor_panels + field_panels

        for i in range(0, len(panels), 2):
            cols2 = st.columns(2)
            for j, (title, cols_list) in enumerate(panels[i:i+2]):
                with cols2[j]:
                    ser = monthly_series_for_cols(cols_list)
                    render_small_line(title, ser, tab_name, i + j)
