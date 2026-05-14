import streamlit as st
import pandas as pd

from core.database import get_cached_data


# ==============================
# 17. ACTIVITY CALENDAR PAGE
# ==============================
def show_calendar():
    
    st.markdown("<h2 style='color: #2B3F87;'>📅 Activity Calendar</h2>", unsafe_allow_html=True)

    loans_df = get_cached_data("loans")

    if loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    loans_df.columns = loans_df.columns.str.strip().str.replace(" ", "_")

    required_keys = ["End_Date", "Total_Repayable", "Status", "Borrower", "Loan_ID", "Principal", "Interest"]
    for col in required_keys:
        if col not in loans_df.columns:
            loans_df[col] = 0 if col in ["Total_Repayable", "Principal", "Interest"] else "Unknown"
    # Convert to proper types for logic
    loans_df["End_Date"] = pd.to_datetime(loans_df["End_Date"], errors="coerce")
    loans_df["Total_Repayable"] = pd.to_numeric(loans_df["Total_Repayable"], errors="coerce").fillna(0)
    
    # Reference date (April 2026)
    today = pd.Timestamp.today().normalize()
    
    # Filter for loans that aren't closed
    active_loans = loans_df[loans_df["Status"].astype(str).str.lower() != "closed"].copy()

    # --- VISUAL CALENDAR WIDGET ---
    calendar_events = []
    for _, r in active_loans.iterrows():
        if pd.notna(r['End_Date']):
            # Color logic: Red for overdue, Blue for upcoming
            is_overdue = r['End_Date'].date() < today.date()
            ev_color = "#FF4B4B" if is_overdue else "#4A90E2"
            
            # Auto-Recovery for display amount if Total_Repayable is zero
            disp_amt = float(r['Total_Repayable']) if r['Total_Repayable'] > 0 else (float(r['Principal']) + float(r['Interest']))
            
            calendar_events.append({
                "title": f"UGX {disp_amt:,.0f} - {r['Borrower']}",
                "start": r['End_Date'].strftime("%Y-%m-%d"),
                "end": r['End_Date'].strftime("%Y-%m-%d"),
                "color": ev_color,
                "allDay": True,
            })

    calendar_options = {
        "headerToolbar": {"left": "prev,next today", "center": "title", "right": "dayGridMonth,timeGridWeek"},
        "initialView": "dayGridMonth",
        "selectable": True,
    }

    # Render the interactive calendar
    calendar(events=calendar_events, options=calendar_options, key="collection_cal")
    
    st.markdown("---")

    # 3. DAILY WORKLOAD METRICS (Zoe Branded Cards)
    # These counts help you see what's happening at a glance
    due_today_df = active_loans[active_loans["End_Date"].dt.date == today.date()]
    upcoming_df = active_loans[
        (active_loans["End_Date"] > today) & 
        (active_loans["End_Date"] <= today + pd.Timedelta(days=7))
    ]
    overdue_count = active_loans[active_loans["End_Date"] < today].shape[0]

    # Create the columns
    m1, m2, m3 = st.columns(3)
    
    # FIX: These must all start at the EXACT same indentation level as the 'm1, m2, m3' line
    m1.markdown(f"""
    <div style="background-color: #ffffff; padding: 20px; border-radius: 15px; border-left: 5px solid #2B3F87; box-shadow: 2px 2px 10px rgba(0,0,0,0.05);">
        <p style="margin:0; font-size:12px; color:#666; font-weight:bold;">DUE TODAY |</p>
        <p style="margin:0; font-size:18px; color:#2B3F87; font-weight:bold;">{len(due_today_df)} Accounts</p>
    </div>
    """, unsafe_allow_html=True)

    m2.markdown(f"""
    <div style="background-color: #F0F8FF; padding: 20px; border-radius: 15px; border-left: 5px solid #2B3F87; box-shadow: 2px 2px 10px rgba(0,0,0,0.05);">
        <p style="margin:0; font-size:12px; color:#666; font-weight:bold;">UPCOMING (7 DAYS) |</p>
        <p style="margin:0; font-size:18px; color:#2B3F87; font-weight:bold;">{len(upcoming_df)} Accounts</p>
    </div>
    """, unsafe_allow_html=True)

    m3.markdown(f"""
    <div style="background-color: #FFF5F5; padding: 20px; border-radius: 15px; border-left: 5px solid #D32F2F; box-shadow: 2px 2px 10px rgba(0,0,0,0.05);">
        <p style="margin:0; font-size:12px; color:#D32F2F; font-weight:bold;">TOTAL OVERDUE |</p>
        <p style="margin:0; font-size:18px; color:#D32F2F; font-weight:bold;">{overdue_count} Accounts</p>
    </div>
    """, unsafe_allow_html=True)

    # --- CALENDAR FOOTER: REVENUE PREVIEW ---
    st.markdown("---")
    st.markdown("<h4 style='color: #2B3F87;'>📊 Revenue Forecast (This Month)</h4>", unsafe_allow_html=True)
    
    current_month = today.month
    this_month_df = active_loans[active_loans["End_Date"].dt.month == current_month]
    total_expected = this_month_df["Total_Repayable"].sum()
    
    f1, f2 = st.columns(2)
    f1.metric("Expected Collections", f"{total_expected:,.0f} UGX")
    f2.metric("Remaining Appointments", len(this_month_df))
    
    st.write("💡 *Tip: Click any blue/red bar on the calendar above to see the specific borrower details.*")

    # --- SECTION: DUE TODAY ---
    st.markdown("<h4 style='color: #2B3F87;'>📌 Action Items for Today</h4>", unsafe_allow_html=True)
    if due_today_df.empty:
        st.success("✨ No deadlines for today. Focus on follow-ups!")
    else:
        today_rows = ""
        for i, r in due_today_df.iterrows():
            bg = "#F0F8FF" if i % 2 == 0 else "#FFFFFF"
            today_rows += f"""<tr style="background-color: {bg}; border-bottom: 1px solid #ddd;"><td style="padding:10px;"><b>#{r['Loan_ID']}</b></td><td style="padding:10px;">{r['Borrower']}</td><td style="padding:10px; text-align:right; font-weight:bold; color:#2B3F87;">{r['Total_Repayable']:,.0f}</td><td style="padding:10px; text-align:center;"><span style="background:#2B3F87; color:white; padding:2px 8px; border-radius:10px; font-size:10px;">💰 COLLECT NOW</span></td></tr>"""
        st.markdown(f"""<div style="border:2px solid #2B3F87; border-radius:10px; overflow:hidden;"><table style="width:100%; border-collapse:collapse; font-family:sans-serif; font-size:12px;"><tr style="background:#2B3F87; color:white;"><th style="padding:10px;">Loan ID</th><th style="padding:10px;">Borrower</th><th style="padding:10px; text-align:right;">Amount Due</th><th style="padding:10px; text-align:center;">Action</th></tr>{today_rows}</table></div>""", unsafe_allow_html=True)

    # --- SECTION: UPCOMING ---
    st.markdown("<br><h4 style='color: #2B3F87;'>⏳ Upcoming Deadlines (Next 7 Days)</h4>", unsafe_allow_html=True)
    if upcoming_df.empty:
        st.info("The next few days look quiet.")
    else:
        upcoming_display = upcoming_df.sort_values("End_Date").copy()
        up_rows = ""
        for i, r in upcoming_display.iterrows():
            bg = "#F0F8FF" if i % 2 == 0 else "#FFFFFF"
            display_amt = float(r.get('Total_Repayable', 0)) or (float(r.get('Principal', 0)) + float(r.get('Interest', 0)))
            up_rows += f"""<tr style="background-color: {bg};"><td style="padding:10px; color:#2B3F87; font-weight:bold;">{r['End_Date'].strftime('%d %b (%a)')}</td><td style="padding:10px;">{r.get('Borrower', 'Unknown')}</td><td style="padding:10px; text-align:right; font-weight:bold;">{display_amt:,.0f} UGX</td><td style="padding:10px; text-align:right; color:#666;">ID: #{r.get('Loan_ID', 'N/A')}</td></tr>"""
        st.markdown(f"""<div style="border:1px solid #2B3F87; border-radius:10px; overflow:hidden;"><table style="width:100%; border-collapse:collapse; font-family:sans-serif; font-size:12px;"><tr style="background:#2B3F87; color:white;"><th style="padding:10px;">Due Date</th><th style="padding:10px;">Borrower</th><th style="padding:10px; text-align:right;">Amount</th><th style="padding:10px; text-align:right;">Ref</th></tr>{up_rows}</table></div>""", unsafe_allow_html=True)

    # --- SECTION: IMMEDIATE FOLLOW-UP ---
    st.markdown("<br><h4 style='color: #FF4B4B;'>🔴 Past Due (Immediate Attention)</h4>", unsafe_allow_html=True)
    overdue_df = active_loans[active_loans["End_Date"] < today].copy()
    if overdue_df.empty:
        st.success("Clean Sheet! No overdue loans found. 🎉")
    else:
        overdue_df["Days_Late"] = (today - overdue_df["End_Date"]).dt.days
        overdue_df = overdue_df.sort_values("Days_Late", ascending=False)
        od_rows = ""
        for i, r in overdue_df.iterrows():
            bg = "#FFF5F5"
            late_color = "#FF4B4B" if r['Days_Late'] > 7 else "#FFA500"
            od_rows += f"""<tr style="background-color: {bg}; border-bottom: 1px solid #FFDADA;"><td style="padding:10px;"><b>#{r['Loan_ID']}</b></td><td style="padding:10px;">{r['Borrower']}</td><td style="padding:10px; text-align:center; font-weight:bold; color:{late_color};">{r['Days_Late']} Days</td><td style="padding:10px; text-align:center;"><span style="background:{late_color}; color:white; padding:2px 8px; border-radius:10px; font-size:10px;">{r['Status']}</span></td></tr>"""
        st.markdown(f"""<div style="border:2px solid #FF4B4B; border-radius:10px; overflow:hidden;"><table style="width:100%; border-collapse:collapse; font-family:sans-serif; font-size:12px;"><tr style="background:#FF4B4B; color:white;"><th style="padding:10px;">Loan ID</th><th style="padding:10px;">Borrower</th><th style="padding:10px; text-align:center;">Late By</th><th style="padding:10px; text-align:center;">Status</th></tr>{od_rows}</table></div>""", unsafe_allow_html=True)
