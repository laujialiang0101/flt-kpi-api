"""
FLT KPI Tracker API
FastAPI backend using pre-aggregated materialized views for fast queries.
"""
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Optional
import asyncpg
import os

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'dpg-d4pr99je5dus73eb5730-a.singapore-postgres.render.com'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'flt_sales_commission_db'),
    'user': os.getenv('DB_USER', 'flt_sales_commission_db_user'),
    'password': os.getenv('DB_PASSWORD', 'Wy0ZP1wjLPsIta0YLpYLeRWgdITbya2m'),
    'ssl': 'require'
}

# Global connection pool
pool: asyncpg.Pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(**DB_CONFIG, min_size=2, max_size=10)
    yield
    await pool.close()


app = FastAPI(
    title="FLT KPI Tracker API",
    description="API for Farmasi Lautan Staff KPI Tracking",
    version="1.0.0",
    lifespan=lifespan
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/api/v1/kpi/me")
async def get_my_dashboard(
    staff_id: str = Query(..., description="Staff ID"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None)
):
    """Get personal KPI dashboard for a staff member."""
    if not start_date:
        start_date = date.today().replace(day=1)
    if not end_date:
        end_date = date.today()

    async with pool.acquire() as conn:
        # Get aggregated KPIs from materialized view
        summary = await conn.fetchrow("""
            SELECT
                staff_id,
                outlet_id,
                SUM(transactions) as transactions,
                SUM(total_sales) as total_sales,
                SUM(gross_profit) as gross_profit,
                SUM(house_brand_sales) as house_brand_sales,
                SUM(focused_1_sales) as focused_1_sales,
                SUM(focused_2_3_sales) as focused_2_3_sales,
                SUM(pwp_clearance_total) as pwp_clearance_total
            FROM analytics.mv_staff_daily_kpi
            WHERE staff_id = $1
              AND sale_date BETWEEN $2 AND $3
            GROUP BY staff_id, outlet_id
        """, staff_id, start_date, end_date)

        if not summary:
            raise HTTPException(status_code=404, detail="No data found")

        # Get staff name
        staff_info = await conn.fetchrow("""
            SELECT "AcSalesmanName" FROM "AcSalesman" WHERE "AcSalesmanID" = $1
        """, staff_id)

        # Get outlet name
        outlet_info = await conn.fetchrow("""
            SELECT "LocationName" FROM "AcLocation" WHERE "AcLocationID" = $1
        """, summary['outlet_id'])

        # Get rankings
        rankings = await conn.fetchrow("""
            SELECT outlet_rank_sales, company_rank_sales, sales_percentile
            FROM analytics.mv_staff_rankings
            WHERE staff_id = $1
              AND month = DATE_TRUNC('month', $2::date)
        """, staff_id, start_date)

        # Get daily breakdown
        daily = await conn.fetch("""
            SELECT sale_date, transactions, total_sales, house_brand_sales
            FROM analytics.mv_staff_daily_kpi
            WHERE staff_id = $1
              AND sale_date BETWEEN $2 AND $3
            ORDER BY sale_date
        """, staff_id, start_date, end_date)

        return {
            "success": True,
            "data": {
                "staff_id": staff_id,
                "staff_name": staff_info['AcSalesmanName'] if staff_info else "Unknown",
                "outlet_id": summary['outlet_id'],
                "outlet_name": outlet_info['LocationName'] if outlet_info else "Unknown",
                "period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                },
                "kpis": {
                    "total_sales": float(summary['total_sales'] or 0),
                    "house_brand": float(summary['house_brand_sales'] or 0),
                    "focused_1": float(summary['focused_1_sales'] or 0),
                    "focused_2_3": float(summary['focused_2_3_sales'] or 0),
                    "pwp_clearance": float(summary['pwp_clearance_total'] or 0),
                    "transactions": int(summary['transactions'] or 0),
                    "gross_profit": float(summary['gross_profit'] or 0)
                },
                "rankings": {
                    "outlet_rank": rankings['outlet_rank_sales'] if rankings else None,
                    "company_rank": rankings['company_rank_sales'] if rankings else None,
                    "percentile": float(rankings['sales_percentile']) if rankings and rankings['sales_percentile'] else None
                },
                "daily": [
                    {
                        "date": row['sale_date'].isoformat(),
                        "sales": float(row['total_sales'] or 0),
                        "house_brand": float(row['house_brand_sales'] or 0)
                    }
                    for row in daily
                ]
            }
        }


@app.get("/api/v1/kpi/leaderboard")
async def get_leaderboard(
    scope: str = Query("outlet", regex="^(outlet|company)$"),
    outlet_id: Optional[str] = Query(None),
    month: Optional[str] = Query(None, description="Month in YYYY-MM format"),
    limit: int = Query(20, ge=1, le=100)
):
    """Get staff rankings leaderboard."""
    if month:
        try:
            period = datetime.strptime(month, "%Y-%m").date()
        except:
            raise HTTPException(status_code=400, detail="Invalid month format")
    else:
        period = date.today()

    async with pool.acquire() as conn:
        if scope == "outlet" and outlet_id:
            rows = await conn.fetch("""
                SELECT
                    r.outlet_rank_sales as rank,
                    r.staff_id,
                    s."AcSalesmanName" as staff_name,
                    r.outlet_id,
                    r.total_sales,
                    r.house_brand_sales,
                    r.sales_percentile
                FROM analytics.mv_staff_rankings r
                LEFT JOIN "AcSalesman" s ON r.staff_id = s."AcSalesmanID"
                WHERE r.month = DATE_TRUNC('month', $1::date)
                  AND r.outlet_id = $2
                ORDER BY r.outlet_rank_sales
                LIMIT $3
            """, period, outlet_id, limit)
        else:
            rows = await conn.fetch("""
                SELECT
                    r.company_rank_sales as rank,
                    r.staff_id,
                    s."AcSalesmanName" as staff_name,
                    r.outlet_id,
                    r.total_sales,
                    r.house_brand_sales,
                    r.sales_percentile
                FROM analytics.mv_staff_rankings r
                LEFT JOIN "AcSalesman" s ON r.staff_id = s."AcSalesmanID"
                WHERE r.month = DATE_TRUNC('month', $1::date)
                ORDER BY r.company_rank_sales
                LIMIT $2
            """, period, limit)

        return {
            "success": True,
            "data": {
                "period": period.strftime("%Y-%m"),
                "scope": scope,
                "rankings": [
                    {
                        "rank": row['rank'],
                        "staff_id": row['staff_id'],
                        "staff_name": row['staff_name'] or "Unknown",
                        "outlet_id": row['outlet_id'],
                        "total_sales": float(row['total_sales'] or 0),
                        "house_brand": float(row['house_brand_sales'] or 0),
                        "percentile": float(row['sales_percentile']) if row['sales_percentile'] else None
                    }
                    for row in rows
                ]
            }
        }


@app.get("/api/v1/kpi/team")
async def get_team_overview(
    outlet_id: str = Query(..., description="Outlet ID"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None)
):
    """Get team overview for a manager."""
    if not start_date:
        start_date = date.today().replace(day=1)
    if not end_date:
        end_date = date.today()

    async with pool.acquire() as conn:
        # Outlet summary
        outlet_summary = await conn.fetchrow("""
            SELECT
                SUM(transactions) as transactions,
                SUM(total_sales) as total_sales,
                SUM(gross_profit) as gross_profit,
                SUM(house_brand_sales) as house_brand_sales,
                SUM(pwp_clearance_total) as pwp_clearance_total
            FROM analytics.mv_outlet_daily_kpi
            WHERE outlet_id = $1
              AND sale_date BETWEEN $2 AND $3
        """, outlet_id, start_date, end_date)

        # Staff performance
        staff = await conn.fetch("""
            SELECT
                k.staff_id,
                s."AcSalesmanName" as staff_name,
                SUM(k.transactions) as transactions,
                SUM(k.total_sales) as total_sales,
                SUM(k.house_brand_sales) as house_brand_sales,
                r.outlet_rank_sales as rank
            FROM analytics.mv_staff_daily_kpi k
            LEFT JOIN "AcSalesman" s ON k.staff_id = s."AcSalesmanID"
            LEFT JOIN analytics.mv_staff_rankings r
                ON k.staff_id = r.staff_id
                AND r.month = DATE_TRUNC('month', $2::date)
            WHERE k.outlet_id = $1
              AND k.sale_date BETWEEN $2 AND $3
            GROUP BY k.staff_id, s."AcSalesmanName", r.outlet_rank_sales
            ORDER BY SUM(k.total_sales) DESC
        """, outlet_id, start_date, end_date)

        return {
            "success": True,
            "data": {
                "outlet_id": outlet_id,
                "period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                },
                "summary": {
                    "total_sales": float(outlet_summary['total_sales'] or 0),
                    "gross_profit": float(outlet_summary['gross_profit'] or 0),
                    "house_brand": float(outlet_summary['house_brand_sales'] or 0),
                    "pwp_clearance": float(outlet_summary['pwp_clearance_total'] or 0),
                    "transactions": int(outlet_summary['transactions'] or 0),
                    "staff_count": len(staff)
                },
                "staff": [
                    {
                        "staff_id": row['staff_id'],
                        "staff_name": row['staff_name'] or "Unknown",
                        "total_sales": float(row['total_sales'] or 0),
                        "house_brand": float(row['house_brand_sales'] or 0),
                        "transactions": int(row['transactions'] or 0),
                        "rank": row['rank']
                    }
                    for row in staff
                ]
            }
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
