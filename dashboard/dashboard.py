"""
# Global Partners Dashboard v6.0
Global Partners - Streamlit Dashboard 
Phase 4: Business Intelligence Dashboard

Reads from Athena (Gold layer) and visualizes all 6 metrics.

Install dependencies on EC2:
    pip install streamlit boto3 pandas plotly pyathena

Run:
    streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import boto3
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import warnings
warnings.filterwarnings("ignore")

# ============================================================
# Configuration
# ============================================================
AWS_REGION    = "us-east-2"
S3_STAGING    = "s3://global-partners-datalake/temp/athena/"
ATHENA_DB     = "global_partners_gold"

# ============================================================
# Page Config
# ============================================================
st.set_page_config(
    page_title  = "Global Partners Analytics",
    page_icon   = "🍽️",
    layout      = "wide",
    initial_sidebar_state = "expanded"
)

# ============================================================
# Athena Connection
# ============================================================
@st.cache_resource
def get_connection():
    return connect(
        s3_staging_dir = S3_STAGING,
        region_name    = AWS_REGION,
        cursor_class   = PandasCursor
    )

@st.cache_data(ttl=3600)
def query(sql):
    conn = get_connection()
    df   = pd.read_sql(sql, conn)
    return df

# ============================================================
# Sidebar Navigation
# ============================================================
st.sidebar.image("https://via.placeholder.com/200x60?text=Global+Partners", width=200)
st.sidebar.title("Navigation")

page = st.sidebar.radio("Select Dashboard", [
    "🏠 Overview",
    "👥 Customer Segmentation",
    "⚠️ Churn Risk",
    "📈 Sales Trends",
    "💳 Loyalty Program",
    "📍 Location Performance",
    "🏷️ Discount Effectiveness"
])

st.sidebar.markdown("---")
st.sidebar.caption("Data refreshed daily at 1:00 AM UTC")
st.sidebar.caption("Source: AWS Glue → S3 Gold → Athena")


# ============================================================
# OVERVIEW PAGE
# ============================================================
if page == "🏠 Overview":
    st.title("🍽️ Global Partners — Business Intelligence Dashboard")
    st.markdown("Real-time insights powered by the AWS data pipeline.")

    # KPI Cards
    df_clv    = query(f"SELECT COUNT(*) as customers, SUM(total_clv) as revenue, AVG(total_clv) as avg_clv FROM {ATHENA_DB}.fact_daily_clv")
    df_churn  = query(f"SELECT churn_risk_tag, COUNT(*) as count FROM {ATHENA_DB}.fact_churn_indicators GROUP BY churn_risk_tag")
    df_seg    = query(f"SELECT segment, COUNT(*) as count FROM {ATHENA_DB}.fact_rfm_segments GROUP BY segment ORDER BY count DESC")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Customers", f"{int(df_clv['customers'].iloc[0]):,}")
    with col2:
        st.metric("Total Revenue", f"${df_clv['revenue'].iloc[0]:,.0f}")
    with col3:
        st.metric("Avg Customer LTV", f"${df_clv['avg_clv'].iloc[0]:,.2f}")
    with col4:
        at_risk = df_churn[df_churn['churn_risk_tag'] == 'At Risk']['count'].sum() if not df_churn.empty else 0
        st.metric("At Risk Customers", f"{int(at_risk):,}", delta="⚠️ Needs attention", delta_color="inverse")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Customer Segments")
        if not df_seg.empty:
            fig = px.pie(df_seg, values='count', names='segment',
                        color_discrete_sequence=px.colors.qualitative.Set3)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Churn Risk Distribution")
        if not df_churn.empty:
            colors = {'Active': '#2ecc71', 'Warning': '#f39c12', 'At Risk': '#e74c3c'}
            fig = px.bar(df_churn, x='churn_risk_tag', y='count',
                        color='churn_risk_tag', color_discrete_map=colors)
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # CLV Tier Distribution
    st.subheader("CLV Tier Breakdown")
    df_tier = query(f"SELECT clv_tier, COUNT(*) as customers, SUM(total_clv) as revenue FROM {ATHENA_DB}.fact_daily_clv GROUP BY clv_tier")
    if not df_tier.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(df_tier, x='clv_tier', y='customers',
                        color='clv_tier',
                        color_discrete_map={'High':'#2ecc71','Medium':'#3498db','Low':'#e74c3c'},
                        title="Customers by CLV Tier")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(df_tier, x='clv_tier', y='revenue',
                        color='clv_tier',
                        color_discrete_map={'High':'#2ecc71','Medium':'#3498db','Low':'#e74c3c'},
                        title="Revenue by CLV Tier")
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# CUSTOMER SEGMENTATION PAGE
# ============================================================
elif page == "👥 Customer Segmentation":
    st.title("👥 Customer Segmentation")
    st.markdown("RFM-based segmentation to enable targeted marketing.")

    df_rfm = query(f"""
        SELECT segment, clv_tier, COUNT(*) as customers,
               AVG(recency_days) as avg_recency,
               AVG(frequency) as avg_frequency,
               AVG(monetary) as avg_monetary,
               AVG(rfm_score) as avg_rfm_score
        FROM {ATHENA_DB}.fact_rfm_segments r
        JOIN {ATHENA_DB}.fact_daily_clv c USING (user_id)
        GROUP BY segment, clv_tier
        ORDER BY customers DESC
    """)

    df_seg_summary = query(f"""
        SELECT segment, COUNT(*) as customers,
               AVG(recency_days) as avg_recency,
               AVG(frequency) as avg_orders,
               AVG(monetary) as avg_spend
        FROM {ATHENA_DB}.fact_rfm_segments
        GROUP BY segment
        ORDER BY customers DESC
    """)

    # Segment overview
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Segment Distribution")
        fig = px.bar(df_seg_summary, x='segment', y='customers',
                    color='segment', text='customers',
                    color_discrete_sequence=px.colors.qualitative.Bold)
        fig.update_traces(textposition='outside')
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Segment Summary")
        if not df_seg_summary.empty:
            display_df = df_seg_summary.copy()
            display_df['avg_spend'] = display_df['avg_spend'].map('${:,.2f}'.format)
            display_df['avg_recency'] = display_df['avg_recency'].map('{:.0f} days'.format)
            display_df['avg_orders'] = display_df['avg_orders'].map('{:.1f}'.format)
            display_df.columns = ['Segment', 'Customers', 'Avg Recency', 'Avg Orders', 'Avg Spend']
            st.dataframe(display_df, use_container_width=True, hide_index=True)

    # RFM Scatter
    st.subheader("RFM Analysis — Recency vs Monetary by Segment")
    df_scatter = query(f"""
        SELECT segment, AVG(recency_days) as recency,
               AVG(frequency) as frequency,
               AVG(monetary) as monetary,
               COUNT(*) as customers
        FROM {ATHENA_DB}.fact_rfm_segments
        GROUP BY segment
    """)
    if not df_scatter.empty:
        fig = px.scatter(df_scatter, x='recency', y='monetary',
                        size='customers', color='segment',
                        hover_name='segment',
                        labels={'recency': 'Avg Days Since Last Order', 'monetary': 'Avg Total Spend ($)'},
                        color_discrete_sequence=px.colors.qualitative.Bold)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # CLV Tier vs Segment heatmap
    st.subheader("Segment vs CLV Tier")
    if not df_rfm.empty:
        pivot = df_rfm.pivot_table(values='customers', index='segment', columns='clv_tier', fill_value=0)
        fig = px.imshow(pivot, text_auto=True, color_continuous_scale='Blues',
                       labels={'color': 'Customers'})
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
# CHURN RISK PAGE
# ============================================================
elif page == "⚠️ Churn Risk":
    st.title("⚠️ Churn Risk Indicators")
    st.markdown("Threshold-based alerts to prompt re-engagement actions.")

    df_churn = query(f"""
        SELECT churn_risk_tag,
               COUNT(*) as customers,
               AVG(days_since_last_order) as avg_days_inactive,
               AVG(avg_order_gap_days) as avg_gap,
               AVG(total_spend) as avg_lifetime_spend,
               AVG(spend_pct_change) as avg_spend_change
        FROM {ATHENA_DB}.fact_churn_indicators
        GROUP BY churn_risk_tag
    """)

    # KPI row
    col1, col2, col3 = st.columns(3)
    for _, row in df_churn.iterrows():
        color = {'Active': '🟢', 'Warning': '🟡', 'At Risk': '🔴'}.get(row['churn_risk_tag'], '⚪')
        if row['churn_risk_tag'] == 'Active':
            col1.metric(f"{color} Active Customers", f"{int(row['customers']):,}",
                       f"Avg {row['avg_days_inactive']:.0f} days since order")
        elif row['churn_risk_tag'] == 'Warning':
            col2.metric(f"{color} Warning", f"{int(row['customers']):,}",
                       f"Avg {row['avg_days_inactive']:.0f} days since order", delta_color="off")
        elif row['churn_risk_tag'] == 'At Risk':
            col3.metric(f"{color} At Risk", f"{int(row['customers']):,}",
                       f"Avg {row['avg_days_inactive']:.0f} days since order", delta_color="inverse")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Inactivity Distribution")
        df_dist = query(f"""
            SELECT CASE
                WHEN days_since_last_order <= 30 THEN '0-30 days'
                WHEN days_since_last_order <= 60 THEN '31-60 days'
                WHEN days_since_last_order <= 90 THEN '61-90 days'
                ELSE '90+ days'
            END as bucket, COUNT(*) as customers
            FROM {ATHENA_DB}.fact_churn_indicators
            GROUP BY 1 ORDER BY 1
        """)
        if not df_dist.empty:
            fig = px.bar(df_dist, x='bucket', y='customers',
                        color='bucket',
                        color_discrete_sequence=['#2ecc71','#f39c12','#e67e22','#e74c3c'])
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Spend Trend for At-Risk Customers")
        df_spend = query(f"""
            SELECT churn_risk_tag,
                   AVG(spend_last_30d) as last_30d,
                   AVG(spend_prior_30d) as prior_30d
            FROM {ATHENA_DB}.fact_churn_indicators
            WHERE spend_prior_30d IS NOT NULL
            GROUP BY churn_risk_tag
        """)
        if not df_spend.empty:
            df_melted = df_spend.melt(id_vars='churn_risk_tag',
                                      value_vars=['last_30d','prior_30d'],
                                      var_name='Period', value_name='Spend')
            df_melted['Period'] = df_melted['Period'].map({'last_30d':'Last 30 Days','prior_30d':'Prior 30 Days'})
            fig = px.bar(df_melted, x='churn_risk_tag', y='Spend', color='Period',
                        barmode='group', color_discrete_sequence=['#3498db','#95a5a6'])
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # At-risk customer table
    st.subheader("At-Risk Customer Details")
    df_at_risk = query(f"""
        SELECT c.user_id, c.days_since_last_order, c.avg_order_gap_days,
               c.total_spend, c.spend_pct_change, c.churn_risk_tag,
               d.total_clv, d.clv_tier, d.total_orders
        FROM {ATHENA_DB}.fact_churn_indicators c
        JOIN {ATHENA_DB}.fact_daily_clv d USING (user_id)
        WHERE c.churn_risk_tag = 'At Risk'
        ORDER BY d.total_clv DESC
        LIMIT 50
    """)
    if not df_at_risk.empty:
        st.dataframe(df_at_risk, use_container_width=True, hide_index=True)


# ============================================================
# SALES TRENDS PAGE
# ============================================================
elif page == "📈 Sales Trends":
    st.title("📈 Sales Trends & Seasonality")
    st.markdown("Weekly and monthly sales aggregates with seasonal patterns.")

    # Monthly revenue trend
    df_monthly = query(f"""
        SELECT year, month, SUM(daily_revenue) as revenue,
               SUM(daily_orders) as orders,
               AVG(avg_order_value) as aov
        FROM {ATHENA_DB}.fact_sales_trends
        GROUP BY year, month
        ORDER BY year, month
    """)

    if not df_monthly.empty:
        st.subheader("Monthly Revenue Trend")
        df_monthly['period'] = df_monthly['month'].astype(str) + ' ' + df_monthly['year'].astype(str)
        fig = px.line(df_monthly, x='period', y='revenue',
                     markers=True, line_shape='spline',
                     labels={'revenue': 'Revenue ($)', 'period': 'Month'})
        fig.update_traces(line_color='#3498db', line_width=2)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by Day of Week")
        df_dow = query(f"""
            SELECT day_of_week, AVG(daily_revenue) as avg_revenue,
                   AVG(daily_orders) as avg_orders
            FROM {ATHENA_DB}.fact_sales_trends
            WHERE day_of_week IS NOT NULL
            GROUP BY day_of_week
            ORDER BY avg_revenue DESC
        """)
        if not df_dow.empty:
            fig = px.bar(df_dow, x='day_of_week', y='avg_revenue',
                        color='avg_revenue', color_continuous_scale='Blues',
                        labels={'avg_revenue': 'Avg Daily Revenue ($)', 'day_of_week': 'Day'})
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Holiday vs Regular Day Revenue")
        df_holiday = query(f"""
            SELECT is_holiday, AVG(daily_revenue) as avg_revenue,
                   COUNT(*) as days
            FROM {ATHENA_DB}.fact_sales_trends
            WHERE is_holiday IS NOT NULL
            GROUP BY is_holiday
        """)
        if not df_holiday.empty:
            df_holiday['label'] = df_holiday['is_holiday'].map({True: 'Holiday', False: 'Regular Day'})
            fig = px.bar(df_holiday, x='label', y='avg_revenue',
                        color='label',
                        color_discrete_map={'Holiday':'#e74c3c','Regular Day':'#3498db'})
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # Top categories
    st.subheader("Revenue by Item Category")
    df_cat = query(f"""
        SELECT item_category, SUM(daily_revenue) as revenue,
               SUM(daily_orders) as orders
        FROM {ATHENA_DB}.fact_sales_trends
        WHERE item_category IS NOT NULL AND item_category != ''
        GROUP BY item_category
        ORDER BY revenue DESC
        LIMIT 15
    """)
    if not df_cat.empty:
        fig = px.bar(df_cat, x='revenue', y='item_category',
                    orientation='h', color='revenue',
                    color_continuous_scale='Blues')
        fig.update_layout(height=450, showlegend=False, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
# LOYALTY PROGRAM PAGE
# ============================================================
elif page == "💳 Loyalty Program":
    st.title("💳 Loyalty Program Impact")
    st.markdown("Comparing loyalty members vs non-members across key metrics.")

    df_loyalty = query(f"SELECT * FROM {ATHENA_DB}.fact_loyalty_comparison")

    if not df_loyalty.empty:
        df_loyalty['label'] = df_loyalty['is_loyalty'].map({True: 'Loyalty Members', False: 'Non-Members'})

        # KPI comparison
        col1, col2, col3, col4 = st.columns(4)
        loyalty = df_loyalty[df_loyalty['is_loyalty'] == True]
        non_loyalty = df_loyalty[df_loyalty['is_loyalty'] == False]

        if not loyalty.empty and not non_loyalty.empty:
            clv_lift = ((loyalty['avg_clv'].iloc[0] - non_loyalty['avg_clv'].iloc[0]) /
                       non_loyalty['avg_clv'].iloc[0] * 100)
            col1.metric("Loyalty Avg CLV", f"${loyalty['avg_clv'].iloc[0]:,.2f}",
                       f"+{clv_lift:.1f}% vs non-members")
            col2.metric("Non-Member Avg CLV", f"${non_loyalty['avg_clv'].iloc[0]:,.2f}")
            col3.metric("Loyalty Avg Orders", f"{loyalty['avg_orders'].iloc[0]:.1f}")
            col4.metric("Non-Member Avg Orders", f"{non_loyalty['avg_orders'].iloc[0]:.1f}")

        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Average CLV Comparison")
            fig = px.bar(df_loyalty, x='label', y='avg_clv',
                        color='label',
                        color_discrete_map={'Loyalty Members':'#2ecc71','Non-Members':'#95a5a6'},
                        text='avg_clv')
            fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Average Order Value Comparison")
            fig = px.bar(df_loyalty, x='label', y='avg_order_value',
                        color='label',
                        color_discrete_map={'Loyalty Members':'#2ecc71','Non-Members':'#95a5a6'},
                        text='avg_order_value')
            fig.update_traces(texttemplate='$%{text:,.2f}', textposition='outside')
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Total Revenue Contribution")
        fig = px.pie(df_loyalty, values='total_revenue', names='label',
                    color='label',
                    color_discrete_map={'Loyalty Members':'#2ecc71','Non-Members':'#95a5a6'})
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
# LOCATION PERFORMANCE PAGE
# ============================================================
elif page == "📍 Location Performance":
    st.title("📍 Location Performance")
    st.markdown("Revenue ranking and operational metrics by restaurant location.")

    df_loc = query(f"""
        SELECT * FROM {ATHENA_DB}.fact_location_performance
        ORDER BY revenue_rank
    """)

    if not df_loc.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Locations", f"{len(df_loc):,}")
        col2.metric("Top Location Revenue", f"${df_loc['total_revenue'].max():,.0f}")
        col3.metric("Avg Revenue per Location", f"${df_loc['total_revenue'].mean():,.0f}")

        st.markdown("---")

        # Top 10 locations
        st.subheader("Top 10 Locations by Revenue")
        df_top = df_loc.head(10)
        fig = px.bar(df_top, x='total_revenue', y='restaurant_id',
                    orientation='h',
                    color='performance_tier',
                    color_discrete_map={
                        'Top Performer':'#2ecc71',
                        'Mid Performer':'#3498db',
                        'Low Performer':'#e74c3c'
                    },
                    text='total_revenue')
        fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
        fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Orders Per Day by Location")
            fig = px.bar(df_top, x='restaurant_id', y='orders_per_day',
                        color='performance_tier',
                        color_discrete_map={
                            'Top Performer':'#2ecc71',
                            'Mid Performer':'#3498db',
                            'Low Performer':'#e74c3c'
                        })
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Performance Tier Distribution")
            tier_counts = df_loc.groupby('performance_tier').size().reset_index(name='count')
            fig = px.pie(tier_counts, values='count', names='performance_tier',
                        color='performance_tier',
                        color_discrete_map={
                            'Top Performer':'#2ecc71',
                            'Mid Performer':'#3498db',
                            'Low Performer':'#e74c3c'
                        })
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Full Location Rankings")
        display_df = df_loc[['revenue_rank','restaurant_id','total_revenue',
                             'avg_order_value','total_orders','orders_per_day','performance_tier']].copy()
        display_df['total_revenue'] = display_df['total_revenue'].map('${:,.0f}'.format)
        display_df['avg_order_value'] = display_df['avg_order_value'].map('${:,.2f}'.format)
        display_df['orders_per_day'] = display_df['orders_per_day'].map('{:.1f}'.format)
        display_df.columns = ['Rank','Location','Revenue','Avg Order','Orders','Orders/Day','Tier']
        st.dataframe(display_df, use_container_width=True, hide_index=True)


# ============================================================
# DISCOUNT EFFECTIVENESS PAGE
# ============================================================
elif page == "🏷️ Discount Effectiveness":
    st.title("🏷️ Pricing & Discount Effectiveness")
    st.markdown("How discounts affect revenue and profitability.")

    df_disc = query(f"""
        SELECT has_discount,
               SUM(order_count) as total_orders,
               SUM(gross_revenue) as gross_revenue,
               SUM(net_revenue) as net_revenue,
               SUM(total_discount_amount) as total_discount,
               AVG(avg_order_value) as avg_order_value,
               AVG(discount_rate_pct) as avg_discount_rate,
               SUM(unique_customers) as customers
        FROM {ATHENA_DB}.fact_discount_effectiveness
        GROUP BY has_discount
    """)

    if not df_disc.empty:
        df_disc['label'] = df_disc['has_discount'].map({True: 'Discounted Orders', False: 'Full Price Orders'})

        col1, col2, col3, col4 = st.columns(4)
        discounted = df_disc[df_disc['has_discount'] == True]
        full_price  = df_disc[df_disc['has_discount'] == False]

        if not discounted.empty:
            col1.metric("Discounted Orders", f"{int(discounted['total_orders'].iloc[0]):,}")
            col2.metric("Avg Discount Rate", f"{discounted['avg_discount_rate'].iloc[0]:.1f}%")
        if not full_price.empty:
            col3.metric("Full Price Orders", f"{int(full_price['total_orders'].iloc[0]):,}")
            col4.metric("Full Price Avg Order", f"${full_price['avg_order_value'].iloc[0]:,.2f}")

        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Gross vs Net Revenue")
            fig = go.Figure(data=[
                go.Bar(name='Gross Revenue', x=df_disc['label'], y=df_disc['gross_revenue'],
                      marker_color=['#e74c3c','#3498db']),
                go.Bar(name='Net Revenue', x=df_disc['label'], y=df_disc['net_revenue'],
                      marker_color=['#c0392b','#2980b9'])
            ])
            fig.update_layout(barmode='group', height=350)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Average Order Value Comparison")
            fig = px.bar(df_disc, x='label', y='avg_order_value',
                        color='label',
                        color_discrete_map={
                            'Discounted Orders':'#e74c3c',
                            'Full Price Orders':'#3498db'
                        },
                        text='avg_order_value')
            fig.update_traces(texttemplate='$%{text:,.2f}', textposition='outside')
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        # Discount trend over time
        st.subheader("Discount Usage Over Time")
        df_trend = query(f"""
            SELECT order_date, has_discount,
                   SUM(net_revenue) as revenue,
                   SUM(order_count) as orders
            FROM {ATHENA_DB}.fact_discount_effectiveness
            GROUP BY order_date, has_discount
            ORDER BY order_date
        """)
        if not df_trend.empty:
            df_trend['label'] = df_trend['has_discount'].map({True: 'Discounted', False: 'Full Price'})
            fig = px.line(df_trend, x='order_date', y='revenue',
                         color='label', line_shape='spline',
                         color_discrete_map={'Discounted':'#e74c3c','Full Price':'#3498db'})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
