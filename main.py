"""
FLT KPI Tracker API
FastAPI backend using pre-aggregated materialized views for fast queries.
"""
from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from starlette.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from typing import Optional, List
from pydantic import BaseModel
import asyncpg
import os
import io
import secrets
import json

# Optional imports for Excel handling
try:
    import openpyxl
    from openpyxl import Workbook
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False


# ============================================================================
# Pydantic Models
# ============================================================================

class LoginRequest(BaseModel):
    code: str
    password: str


class LoginResponse(BaseModel):
    success: bool
    user: Optional[dict] = None
    token: Optional[str] = None
    error: Optional[str] = None


class TargetUploadRow(BaseModel):
    staff_id: str
    year_month: int  # YYYYMM format
    total_sales: float = 0
    house_brand: float = 0
    focused_1: float = 0
    focused_2: float = 0
    focused_3: float = 0
    pwp: float = 0
    clearance: float = 0
    transactions: int = 0


# ============================================================================
# Role Mapping
# ============================================================================

ROLE_MAPPING = {
    'ADMINISTRATORS': 'admin',
    'SUPERVISOR': 'supervisor',
    'PIC OUTLET': 'pic',
    'PIC': 'pic',
    'AREA_MANAGER': 'area_manager',
    'AREA MANAGER': 'area_manager',
    'OPERATIONS_MANAGER': 'operations_manager',
    'OPERATIONS MANAGER': 'operations_manager',
    'CASHIER': 'staff',
    'PRICE CHECKER': 'staff',
}

ROLE_PERMISSIONS = {
    'admin': {
        'can_view_own_kpi': True,
        'can_view_leaderboard': True,
        'can_submit_audit': True,
        'can_upload_targets': True,
        'can_view_all_staff': True,
        'can_manage_roles': True
    },
    'operations_manager': {
        'can_view_own_kpi': True,
        'can_view_leaderboard': True,
        'can_submit_audit': True,
        'can_upload_targets': True,
        'can_view_all_staff': True,
        'can_manage_roles': False
    },
    'area_manager': {
        'can_view_own_kpi': True,
        'can_view_leaderboard': True,
        'can_submit_audit': True,
        'can_upload_targets': False,
        'can_view_all_staff': True,
        'can_manage_roles': False
    },
    'supervisor': {
        'can_view_own_kpi': True,
        'can_view_leaderboard': True,
        'can_submit_audit': True,
        'can_upload_targets': False,
        'can_view_all_staff': True,
        'can_manage_roles': False
    },
    'pic': {
        'can_view_own_kpi': True,
        'can_view_leaderboard': True,
        'can_submit_audit': False,
        'can_upload_targets': False,
        'can_view_all_staff': True,
        'can_manage_roles': False
    },
    'staff': {
        'can_view_own_kpi': True,
        'can_view_leaderboard': True,
        'can_submit_audit': False,
        'can_upload_targets': False,
        'can_view_all_staff': False,
        'can_manage_roles': False
    }
}

# In-memory session store (for production, use Redis)
sessions = {}

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

# GZip compression for responses
app.add_middleware(GZipMiddleware, minimum_size=500)


# ============================================================================
# Helper Functions
# ============================================================================

def get_role_from_group(user_group: str, is_supervisor: bool) -> str:
    """Map AcPOSUserGroupID to application role."""
    group = (user_group or '').upper().strip()
    role = ROLE_MAPPING.get(group, 'staff')
    # Supervisor flag can override role
    if is_supervisor and role not in ['admin', 'operations_manager']:
        role = 'supervisor'
    return role


async def get_current_user(token: str = Query(None, alias="token")):
    """Validate session token and return user info."""
    if not token or token not in sessions:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    session = sessions[token]
    # Check expiry
    if datetime.now() > session['expires_at']:
        del sessions[token]
        raise HTTPException(status_code=401, detail="Session expired")
    return session['user']


# ============================================================================
# Authentication Endpoints
# ============================================================================

@app.post("/api/v1/auth/login")
async def login(request: LoginRequest):
    """Authenticate user with POS credentials."""
    try:
        async with pool.acquire() as conn:
            # Query AcPersonal table for credentials
            user = await conn.fetchrow("""
                SELECT
                    "Code" as code,
                    "Name" as name,
                    "Password" as password,
                    "Active" as active,
                    "IsSupervisor" as is_supervisor,
                    "AcPOSUserGroupID" as user_group
                FROM "AcPersonal"
                WHERE UPPER("Code") = UPPER($1)
                  AND "Active" = 'Y'
            """, request.code)

            if not user:
                return {"success": False, "error": "Invalid credentials or inactive account"}

            # Check password (plain text comparison - legacy system)
            if user['password'] != request.password:
                return {"success": False, "error": "Invalid credentials"}

            # Get staff's group_id (assigned team) and outlet_id (physical work location)
            staff_info = await conn.fetchrow("""
                SELECT
                    s."AcSalesmanGroupID" as group_id,
                    COALESCE(
                        (SELECT k.outlet_id FROM analytics.mv_staff_daily_kpi k
                         WHERE k.staff_id = $1
                         ORDER BY k.sale_date DESC LIMIT 1),
                        s."AcSalesmanGroupID"
                    ) as outlet_id
                FROM "AcSalesman" s
                WHERE s."AcSalesmanID" = $1
                  AND s."Active" = 'Y'
            """, user['code'])

            # Determine role and permissions
            is_supervisor = user['is_supervisor'] == 'Y'
            role = get_role_from_group(user['user_group'], is_supervisor)
            permissions = ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS['staff'])

            # Generate session token
            token = secrets.token_urlsafe(32)

            # Store session (expires in 24 hours)
            # outlet_id = physical location where sales are made (for summary)
            # group_id = assigned team (for staff filtering)
            user_data = {
                'code': user['code'],
                'name': user['name'],
                'role': role,
                'outlet_id': staff_info['outlet_id'] if staff_info else None,
                'group_id': staff_info['group_id'] if staff_info else None,
                'is_supervisor': is_supervisor,
                'user_group': user['user_group'],
                'permissions': permissions
            }

            sessions[token] = {
                'user': user_data,
                'expires_at': datetime.now() + timedelta(hours=24)
            }

            return {
                "success": True,
                "user": user_data,
                "token": token
            }

    except Exception as e:
        return {"success": False, "error": f"Login failed: {str(e)}"}


@app.get("/api/v1/auth/session")
async def get_session(token: str = Query(..., description="Session token")):
    """Validate session and return user info."""
    if token not in sessions:
        return {"success": False, "error": "Invalid session"}

    session = sessions[token]
    if datetime.now() > session['expires_at']:
        del sessions[token]
        return {"success": False, "error": "Session expired"}

    return {"success": True, "user": session['user']}


@app.post("/api/v1/auth/logout")
async def logout(token: str = Query(..., description="Session token")):
    """Logout and invalidate session."""
    if token in sessions:
        del sessions[token]
    return {"success": True, "message": "Logged out successfully"}


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
    """Get personal KPI dashboard for a staff member - REAL-TIME from base tables.

    Includes BOTH cash sales (AcCSD) AND invoice sales (AcCusInvoiceD).
    """
    if not start_date:
        start_date = date.today().replace(day=1)
    if not end_date:
        end_date = date.today()

    try:
        async with pool.acquire() as conn:
            # REAL-TIME: Query base tables directly for live data
            # Combines BOTH cash sales (AcCSD) and invoice sales (AcCusInvoiceD)
            summary = await conn.fetchrow("""
                WITH combined_sales AS (
                    -- Cash Sales (AcCSD + AcCSM)
                    SELECT
                        d."AcSalesmanID" AS staff_id,
                        m."AcLocationID" AS outlet_id,
                        m."DocumentNo" AS doc_no,
                        d."ItemTotal" AS amount,
                        COALESCE(d."ItemCost", 0) AS cost,
                        s."AcStockUDGroup1ID" AS stock_group,
                        d."AcStockID",
                        d."AcStockUOMID"
                    FROM "AcCSD" d
                    INNER JOIN "AcCSM" m ON d."DocumentNo" = m."DocumentNo"
                    LEFT JOIN "AcStockCompany" s ON d."AcStockID" = s."AcStockID" AND d."AcStockUOMID" = s."AcStockUOMID"
                    WHERE d."AcSalesmanID" = $1
                      AND m."DocumentDate"::date BETWEEN $2 AND $3

                    UNION ALL

                    -- Invoice Sales (AcCusInvoiceD + AcCusInvoiceM)
                    SELECT
                        d."AcSalesmanID" AS staff_id,
                        m."AcLocationID" AS outlet_id,
                        m."AcCusInvoiceMID" AS doc_no,
                        d."ItemTotalPrice" AS amount,
                        0 AS cost,
                        s."AcStockUDGroup1ID" AS stock_group,
                        d."AcStockID",
                        d."AcStockUOMID"
                    FROM "AcCusInvoiceD" d
                    INNER JOIN "AcCusInvoiceM" m ON d."AcCusInvoiceMID" = m."AcCusInvoiceMID"
                    LEFT JOIN "AcStockCompany" s ON d."AcStockID" = s."AcStockID" AND d."AcStockUOMID" = s."AcStockUOMID"
                    WHERE d."AcSalesmanID" = $1
                      AND m."DocumentDate"::date BETWEEN $2 AND $3
                ),
                sales_data AS (
                    SELECT
                        staff_id,
                        MAX(outlet_id) AS outlet_id,
                        COUNT(DISTINCT doc_no) AS transactions,
                        SUM(amount) AS total_sales,
                        SUM(amount - cost) AS gross_profit,
                        SUM(CASE WHEN stock_group = 'HOUSE BRAND' THEN amount ELSE 0 END) AS house_brand_sales,
                        SUM(CASE WHEN stock_group = 'FOCUSED ITEM 1' THEN amount ELSE 0 END) AS focused_1_sales,
                        SUM(CASE WHEN stock_group = 'FOCUSED ITEM 2' THEN amount ELSE 0 END) AS focused_2_sales,
                        SUM(CASE WHEN stock_group = 'FOCUSED ITEM 3' THEN amount ELSE 0 END) AS focused_3_sales,
                        SUM(CASE WHEN stock_group = 'STOCK CLEARANCE' THEN amount ELSE 0 END) AS clearance_sales
                    FROM combined_sales
                    GROUP BY staff_id
                ),
                pwp_data AS (
                    SELECT
                        d."AcSalesmanID" AS staff_id,
                        SUM(d."ItemTotal") AS pwp_sales
                    FROM "AcCSDPromotionType" pt
                    INNER JOIN "AcCSD" d ON pt."DocumentNo" = d."DocumentNo" AND pt."ItemNo" = d."ItemNo"
                    INNER JOIN "AcCSM" m ON d."DocumentNo" = m."DocumentNo"
                    WHERE pt."AcPromotionSettingID" = 'PURCHASE WITH PURCHASE'
                      AND d."AcSalesmanID" = $1
                      AND m."DocumentDate"::date BETWEEN $2 AND $3
                    GROUP BY d."AcSalesmanID"
                )
                SELECT
                    sd.staff_id, sd.outlet_id, sd.transactions,
                    ROUND(sd.total_sales, 2) AS total_sales,
                    ROUND(sd.gross_profit, 2) AS gross_profit,
                    ROUND(sd.house_brand_sales, 2) AS house_brand_sales,
                    ROUND(sd.focused_1_sales, 2) AS focused_1_sales,
                    ROUND(sd.focused_2_sales, 2) AS focused_2_sales,
                    ROUND(sd.focused_3_sales, 2) AS focused_3_sales,
                    ROUND(COALESCE(pd.pwp_sales, 0), 2) AS pwp_sales,
                    ROUND(sd.clearance_sales, 2) AS clearance_sales
                FROM sales_data sd
                LEFT JOIN pwp_data pd ON sd.staff_id = pd.staff_id
            """, staff_id, start_date, end_date)

            if not summary:
                raise HTTPException(status_code=404, detail="No data found")

            # Get staff name
            staff_info = await conn.fetchrow("""
                SELECT "AcSalesmanName" FROM "AcSalesman" WHERE "AcSalesmanID" = $1
            """, staff_id)

            # Get outlet name
            outlet_info = await conn.fetchrow("""
                SELECT "AcLocationDesc" as outlet_name FROM "AcLocation" WHERE "AcLocationID" = $1
            """, summary['outlet_id'])

            # Get rankings
            rankings = await conn.fetchrow("""
                SELECT outlet_rank_sales, company_rank_sales, sales_percentile
                FROM analytics.mv_staff_rankings
                WHERE staff_id = $1
                  AND month = DATE_TRUNC('month', $2::date)
            """, staff_id, start_date)

            # Get daily breakdown - REAL-TIME from base tables (includes invoice sales)
            daily = await conn.fetch("""
                WITH combined_daily AS (
                    -- Cash Sales
                    SELECT
                        m."DocumentDate"::date AS sale_date,
                        m."DocumentNo" AS doc_no,
                        d."ItemTotal" AS amount,
                        s."AcStockUDGroup1ID" AS stock_group
                    FROM "AcCSD" d
                    INNER JOIN "AcCSM" m ON d."DocumentNo" = m."DocumentNo"
                    LEFT JOIN "AcStockCompany" s ON d."AcStockID" = s."AcStockID" AND d."AcStockUOMID" = s."AcStockUOMID"
                    WHERE d."AcSalesmanID" = $1
                      AND m."DocumentDate"::date BETWEEN $2 AND $3

                    UNION ALL

                    -- Invoice Sales
                    SELECT
                        m."DocumentDate"::date AS sale_date,
                        m."AcCusInvoiceMID" AS doc_no,
                        d."ItemTotalPrice" AS amount,
                        s."AcStockUDGroup1ID" AS stock_group
                    FROM "AcCusInvoiceD" d
                    INNER JOIN "AcCusInvoiceM" m ON d."AcCusInvoiceMID" = m."AcCusInvoiceMID"
                    LEFT JOIN "AcStockCompany" s ON d."AcStockID" = s."AcStockID" AND d."AcStockUOMID" = s."AcStockUOMID"
                    WHERE d."AcSalesmanID" = $1
                      AND m."DocumentDate"::date BETWEEN $2 AND $3
                )
                SELECT
                    sale_date,
                    COUNT(DISTINCT doc_no) AS transactions,
                    SUM(amount) AS total_sales,
                    SUM(CASE WHEN stock_group = 'HOUSE BRAND' THEN amount ELSE 0 END) AS house_brand_sales
                FROM combined_daily
                GROUP BY sale_date
                ORDER BY sale_date
            """, staff_id, start_date, end_date)

            return {
                "success": True,
                "data": {
                    "staff_id": staff_id,
                    "staff_name": staff_info['AcSalesmanName'] if staff_info else "Unknown",
                    "outlet_id": summary['outlet_id'],
                    "outlet_name": outlet_info['outlet_name'] if outlet_info else "Unknown",
                    "period": {
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat()
                    },
                    "kpis": {
                        "total_sales": float(summary['total_sales'] or 0),
                        "house_brand": float(summary['house_brand_sales'] or 0),
                        "focused_1": float(summary['focused_1_sales'] or 0),
                        "focused_2": float(summary['focused_2_sales'] or 0),
                        "focused_3": float(summary['focused_3_sales'] or 0),
                        "pwp": float(summary['pwp_sales'] or 0),
                        "clearance": float(summary['clearance_sales'] or 0),
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/api/v1/kpi/leaderboard")
async def get_leaderboard(
    scope: str = Query("outlet", regex="^(outlet|company)$"),
    outlet_id: Optional[str] = Query(None),
    month: Optional[str] = Query(None, description="Month in YYYY-MM format"),
    staff_id: Optional[str] = Query(None, description="Current user's staff ID to include their position"),
    limit: int = Query(20, ge=1, le=100)
):
    """Get staff rankings leaderboard. Returns top N + logged-in user's position if not in top N."""
    if month:
        try:
            period = datetime.strptime(month, "%Y-%m").date()
        except:
            raise HTTPException(status_code=400, detail="Invalid month format")
    else:
        period = date.today()

    async with pool.acquire() as conn:
        if scope == "outlet" and outlet_id:
            # Outlet scope: Show staff ASSIGNED to this outlet (by AcSalesmanGroupID)
            # with their aggregated sales across all outlets they worked at
            rows = await conn.fetch("""
                WITH outlet_staff_rankings AS (
                    SELECT
                        s."AcSalesmanID" as staff_id,
                        s."AcSalesmanName" as staff_name,
                        s."AcSalesmanGroupID" as assigned_outlet,
                        COALESCE(SUM(r.total_sales), 0) as total_sales,
                        COALESCE(SUM(r.house_brand_sales), 0) as house_brand_sales,
                        COALESCE(SUM(r.focused_1_sales), 0) as focused_1_sales,
                        COALESCE(SUM(r.focused_2_sales), 0) as focused_2_sales,
                        COALESCE(SUM(r.focused_3_sales), 0) as focused_3_sales,
                        COALESCE(SUM(r.pwp_sales), 0) as pwp_sales,
                        COALESCE(SUM(r.clearance_sales), 0) as clearance_sales,
                        COALESCE(SUM(r.transactions), 0) as transactions
                    FROM "AcSalesman" s
                    LEFT JOIN analytics.mv_staff_rankings r
                        ON s."AcSalesmanID" = r.staff_id
                        AND r.month = DATE_TRUNC('month', $1::date)
                    WHERE s."AcSalesmanGroupID" = $2
                      AND s."Active" = 'Y'
                    GROUP BY s."AcSalesmanID", s."AcSalesmanName", s."AcSalesmanGroupID"
                ),
                ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (ORDER BY total_sales DESC) as rank,
                        PERCENT_RANK() OVER (ORDER BY total_sales) * 100 as sales_percentile
                    FROM outlet_staff_rankings
                )
                SELECT * FROM ranked
                ORDER BY rank
                LIMIT $3
            """, period, outlet_id, limit)

            # If staff_id provided, check if they're in the results, if not get their position
            user_position = None
            if staff_id:
                staff_ids_in_results = [row['staff_id'] for row in rows]
                if staff_id not in staff_ids_in_results:
                    user_row = await conn.fetchrow("""
                        WITH outlet_staff_rankings AS (
                            SELECT
                                s."AcSalesmanID" as staff_id,
                                s."AcSalesmanName" as staff_name,
                                s."AcSalesmanGroupID" as assigned_outlet,
                                COALESCE(SUM(r.total_sales), 0) as total_sales,
                                COALESCE(SUM(r.house_brand_sales), 0) as house_brand_sales,
                                COALESCE(SUM(r.focused_1_sales), 0) as focused_1_sales,
                                COALESCE(SUM(r.focused_2_sales), 0) as focused_2_sales,
                                COALESCE(SUM(r.focused_3_sales), 0) as focused_3_sales,
                                COALESCE(SUM(r.pwp_sales), 0) as pwp_sales,
                                COALESCE(SUM(r.clearance_sales), 0) as clearance_sales,
                                COALESCE(SUM(r.transactions), 0) as transactions
                            FROM "AcSalesman" s
                            LEFT JOIN analytics.mv_staff_rankings r
                                ON s."AcSalesmanID" = r.staff_id
                                AND r.month = DATE_TRUNC('month', $1::date)
                            WHERE s."AcSalesmanGroupID" = $2
                              AND s."Active" = 'Y'
                            GROUP BY s."AcSalesmanID", s."AcSalesmanName", s."AcSalesmanGroupID"
                        ),
                        ranked AS (
                            SELECT *,
                                ROW_NUMBER() OVER (ORDER BY total_sales DESC) as rank,
                                PERCENT_RANK() OVER (ORDER BY total_sales) * 100 as sales_percentile
                            FROM outlet_staff_rankings
                        )
                        SELECT * FROM ranked WHERE staff_id = $3
                    """, period, outlet_id, staff_id)
                    if user_row:
                        user_position = {
                            "rank": user_row['rank'],
                            "staff_id": user_row['staff_id'],
                            "staff_name": user_row['staff_name'] or "Unknown",
                            "outlet_id": user_row['assigned_outlet'],
                            "total_sales": float(user_row['total_sales'] or 0),
                            "house_brand": float(user_row['house_brand_sales'] or 0),
                            "focused_1": float(user_row['focused_1_sales'] or 0),
                            "focused_2": float(user_row['focused_2_sales'] or 0),
                            "focused_3": float(user_row['focused_3_sales'] or 0),
                            "pwp": float(user_row['pwp_sales'] or 0),
                            "clearance": float(user_row['clearance_sales'] or 0),
                            "transactions": int(user_row['transactions'] or 0),
                            "percentile": float(user_row['sales_percentile']) if user_row['sales_percentile'] else None
                        }
        else:
            # Company scope: Show all staff ranked company-wide
            rows = await conn.fetch("""
                SELECT
                    r.company_rank_sales as rank,
                    r.staff_id,
                    s."AcSalesmanName" as staff_name,
                    s."AcSalesmanGroupID" as outlet_id,
                    r.total_sales,
                    r.house_brand_sales,
                    r.focused_1_sales,
                    r.focused_2_sales,
                    r.focused_3_sales,
                    r.pwp_sales,
                    r.clearance_sales,
                    r.transactions,
                    r.sales_percentile
                FROM analytics.mv_staff_rankings r
                LEFT JOIN "AcSalesman" s ON r.staff_id = s."AcSalesmanID"
                WHERE r.month = DATE_TRUNC('month', $1::date)
                ORDER BY r.company_rank_sales
                LIMIT $2
            """, period, limit)

            # If staff_id provided, check if they're in the results
            user_position = None
            if staff_id:
                staff_ids_in_results = [row['staff_id'] for row in rows]
                if staff_id not in staff_ids_in_results:
                    user_row = await conn.fetchrow("""
                        SELECT
                            r.company_rank_sales as rank,
                            r.staff_id,
                            s."AcSalesmanName" as staff_name,
                            s."AcSalesmanGroupID" as outlet_id,
                            r.total_sales,
                            r.house_brand_sales,
                            r.focused_1_sales,
                            r.focused_2_sales,
                            r.focused_3_sales,
                            r.pwp_sales,
                            r.clearance_sales,
                            r.transactions,
                            r.sales_percentile
                        FROM analytics.mv_staff_rankings r
                        LEFT JOIN "AcSalesman" s ON r.staff_id = s."AcSalesmanID"
                        WHERE r.month = DATE_TRUNC('month', $1::date)
                          AND r.staff_id = $2
                    """, period, staff_id)
                    if user_row:
                        user_position = {
                            "rank": user_row['rank'],
                            "staff_id": user_row['staff_id'],
                            "staff_name": user_row['staff_name'] or "Unknown",
                            "outlet_id": user_row['outlet_id'],
                            "total_sales": float(user_row['total_sales'] or 0),
                            "house_brand": float(user_row['house_brand_sales'] or 0),
                            "focused_1": float(user_row['focused_1_sales'] or 0),
                            "focused_2": float(user_row['focused_2_sales'] or 0),
                            "focused_3": float(user_row['focused_3_sales'] or 0),
                            "pwp": float(user_row['pwp_sales'] or 0),
                            "clearance": float(user_row['clearance_sales'] or 0),
                            "transactions": int(user_row['transactions'] or 0),
                            "percentile": float(user_row['sales_percentile']) if user_row['sales_percentile'] else None
                        }

        response_data = {
            "period": period.strftime("%Y-%m"),
            "scope": scope,
            "rankings": [
                {
                    "rank": row['rank'],
                    "staff_id": row['staff_id'],
                    "staff_name": row['staff_name'] or "Unknown",
                    "outlet_id": row.get('outlet_id') or row.get('assigned_outlet'),
                    "total_sales": float(row['total_sales'] or 0),
                    "house_brand": float(row['house_brand_sales'] or 0),
                    "focused_1": float(row['focused_1_sales'] or 0),
                    "focused_2": float(row['focused_2_sales'] or 0),
                    "focused_3": float(row['focused_3_sales'] or 0),
                    "pwp": float(row['pwp_sales'] or 0),
                    "clearance": float(row['clearance_sales'] or 0),
                    "transactions": int(row['transactions'] or 0),
                    "percentile": float(row['sales_percentile']) if row['sales_percentile'] else None
                }
                for row in rows
            ]
        }

        if user_position:
            response_data["user_position"] = user_position

        return {
            "success": True,
            "data": response_data
        }


@app.get("/api/v1/kpi/team")
async def get_team_overview(
    outlet_id: str = Query(..., description="Physical outlet ID for summary"),
    group_id: Optional[str] = Query(None, description="Staff group ID for filtering (defaults to outlet_id)"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None)
):
    """Get team overview for a manager.

    - outlet_id: Physical outlet where sales are made (for summary totals)
    - group_id: Staff team/group for filtering assigned staff (defaults to outlet_id)
    """
    if not start_date:
        start_date = date.today().replace(day=1)
    if not end_date:
        end_date = date.today()

    # If group_id not provided, use outlet_id for backwards compatibility
    staff_group = group_id or outlet_id

    async with pool.acquire() as conn:
        # Outlet summary - ALL sales at this physical outlet (regardless of who made them)
        outlet_summary = await conn.fetchrow("""
            SELECT
                COALESCE(SUM(transactions), 0) as transactions,
                COALESCE(SUM(total_sales), 0) as total_sales,
                COALESCE(SUM(gross_profit), 0) as gross_profit,
                COALESCE(SUM(house_brand_sales), 0) as house_brand_sales,
                COALESCE(SUM(focused_1_sales), 0) as focused_1_sales,
                COALESCE(SUM(focused_2_sales), 0) as focused_2_sales,
                COALESCE(SUM(focused_3_sales), 0) as focused_3_sales,
                COALESCE(SUM(pwp_sales), 0) as pwp_sales,
                COALESCE(SUM(clearance_sales), 0) as clearance_sales
            FROM analytics.mv_outlet_daily_kpi
            WHERE outlet_id = $1
              AND sale_date BETWEEN $2 AND $3
        """, outlet_id, start_date, end_date)

        # Get outlet name
        outlet_info = await conn.fetchrow("""
            SELECT "AcLocationDesc" as outlet_name
            FROM "AcLocation"
            WHERE "AcLocationID" = $1
        """, outlet_id)

        # Staff performance - all 8 KPIs
        # Filter by AcSalesmanGroupID (staff's assigned team/group)
        staff = await conn.fetch("""
            SELECT
                s."AcSalesmanID" as staff_id,
                s."AcSalesmanName" as staff_name,
                COALESCE(SUM(k.transactions), 0) as transactions,
                COALESCE(SUM(k.total_sales), 0) as total_sales,
                COALESCE(SUM(k.house_brand_sales), 0) as house_brand_sales,
                COALESCE(SUM(k.focused_1_sales), 0) as focused_1_sales,
                COALESCE(SUM(k.focused_2_sales), 0) as focused_2_sales,
                COALESCE(SUM(k.focused_3_sales), 0) as focused_3_sales,
                COALESCE(SUM(k.pwp_sales), 0) as pwp_sales,
                COALESCE(SUM(k.clearance_sales), 0) as clearance_sales,
                r.outlet_rank_sales as rank
            FROM "AcSalesman" s
            LEFT JOIN analytics.mv_staff_daily_kpi k
                ON s."AcSalesmanID" = k.staff_id
                AND k.sale_date BETWEEN $2 AND $3
            LEFT JOIN analytics.mv_staff_rankings r
                ON s."AcSalesmanID" = r.staff_id
                AND r.outlet_id = $1
                AND r.month = DATE_TRUNC('month', $2::date)
            WHERE s."AcSalesmanGroupID" = $4
              AND s."Active" = 'Y'
            GROUP BY s."AcSalesmanID", s."AcSalesmanName", r.outlet_rank_sales
            ORDER BY COALESCE(SUM(k.total_sales), 0) DESC
        """, outlet_id, start_date, end_date, staff_group)

        return {
            "success": True,
            "data": {
                "outlet_id": outlet_id,
                "group_id": staff_group,
                "outlet_name": outlet_info['outlet_name'] if outlet_info else outlet_id,
                "period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                },
                "summary": {
                    "total_sales": float(outlet_summary['total_sales'] or 0),
                    "gross_profit": float(outlet_summary['gross_profit'] or 0),
                    "house_brand": float(outlet_summary['house_brand_sales'] or 0),
                    "focused_1": float(outlet_summary['focused_1_sales'] or 0),
                    "focused_2": float(outlet_summary['focused_2_sales'] or 0),
                    "focused_3": float(outlet_summary['focused_3_sales'] or 0),
                    "pwp": float(outlet_summary['pwp_sales'] or 0),
                    "clearance": float(outlet_summary['clearance_sales'] or 0),
                    "transactions": int(outlet_summary['transactions'] or 0),
                    "staff_count": len(staff)
                },
                "staff": [
                    {
                        "staff_id": row['staff_id'],
                        "staff_name": row['staff_name'] or "Unknown",
                        "total_sales": float(row['total_sales'] or 0),
                        "house_brand": float(row['house_brand_sales'] or 0),
                        "focused_1": float(row['focused_1_sales'] or 0),
                        "focused_2": float(row['focused_2_sales'] or 0),
                        "focused_3": float(row['focused_3_sales'] or 0),
                        "pwp": float(row['pwp_sales'] or 0),
                        "clearance": float(row['clearance_sales'] or 0),
                        "transactions": int(row['transactions'] or 0),
                        "rank": row['rank']
                    }
                    for row in staff
                ]
            }
        }


# ============================================================================
# Target Management Endpoints
# ============================================================================

@app.get("/api/v1/targets/me")
async def get_my_targets(
    staff_id: str = Query(..., description="Staff ID"),
    month: Optional[str] = Query(None, description="Month in YYYY-MM format")
):
    """Get staff's targets with current progress."""
    if month:
        try:
            period = datetime.strptime(month, "%Y-%m")
            year_month = int(period.strftime("%Y%m"))
        except:
            raise HTTPException(status_code=400, detail="Invalid month format")
    else:
        period = datetime.now()
        year_month = int(period.strftime("%Y%m"))

    start_date = period.replace(day=1).date()
    # Get last day of month
    if period.month == 12:
        end_date = period.replace(year=period.year+1, month=1, day=1).date() - timedelta(days=1)
    else:
        end_date = period.replace(month=period.month+1, day=1).date() - timedelta(days=1)

    try:
        async with pool.acquire() as conn:
            # Get targets from KPITargets table
            targets = await conn.fetchrow("""
                SELECT
                    total_sales_target,
                    house_brand_target,
                    focused_item_1_target,
                    focused_item_2_target,
                    focused_item_3_target,
                    stock_clearance_target,
                    pwp_target,
                    transaction_count_target
                FROM "KPITargets"
                WHERE salesman_id = $1 AND year_month = $2
            """, staff_id, year_month)

            # Get current KPI values
            current = await conn.fetchrow("""
                SELECT
                    SUM(total_sales) as total_sales,
                    SUM(house_brand_sales) as house_brand,
                    SUM(focused_1_sales) as focused_1,
                    SUM(COALESCE(focused_2_sales, 0)) as focused_2,
                    SUM(COALESCE(focused_3_sales, 0)) as focused_3,
                    SUM(COALESCE(clearance_sales, 0)) as clearance,
                    SUM(COALESCE(pwp_sales, 0)) as pwp,
                    SUM(transactions) as transactions
                FROM analytics.mv_staff_daily_kpi
                WHERE staff_id = $1
                  AND sale_date BETWEEN $2 AND $3
            """, staff_id, start_date, end_date)

            def calc_progress(current_val, target_val):
                if not target_val or target_val == 0:
                    return None
                return round((float(current_val or 0) / float(target_val)) * 100, 1)

            result = {
                "total_sales": {
                    "target": float(targets['total_sales_target'] or 0) if targets else 0,
                    "current": float(current['total_sales'] or 0) if current else 0,
                    "progress": calc_progress(current['total_sales'] if current else 0,
                                            targets['total_sales_target'] if targets else 0)
                },
                "house_brand": {
                    "target": float(targets['house_brand_target'] or 0) if targets else 0,
                    "current": float(current['house_brand'] or 0) if current else 0,
                    "progress": calc_progress(current['house_brand'] if current else 0,
                                            targets['house_brand_target'] if targets else 0)
                },
                "focused_1": {
                    "target": float(targets['focused_item_1_target'] or 0) if targets else 0,
                    "current": float(current['focused_1'] or 0) if current else 0,
                    "progress": calc_progress(current['focused_1'] if current else 0,
                                            targets['focused_item_1_target'] if targets else 0)
                },
                "focused_2": {
                    "target": float(targets['focused_item_2_target'] or 0) if targets else 0,
                    "current": float(current['focused_2'] or 0) if current else 0,
                    "progress": calc_progress(current['focused_2'] if current else 0,
                                            targets['focused_item_2_target'] if targets else 0)
                },
                "focused_3": {
                    "target": float(targets['focused_item_3_target'] or 0) if targets else 0,
                    "current": float(current['focused_3'] or 0) if current else 0,
                    "progress": calc_progress(current['focused_3'] if current else 0,
                                            targets['focused_item_3_target'] if targets else 0)
                },
                "clearance": {
                    "target": float(targets['stock_clearance_target'] or 0) if targets else 0,
                    "current": float(current['clearance'] or 0) if current else 0,
                    "progress": calc_progress(current['clearance'] if current else 0,
                                            targets['stock_clearance_target'] if targets else 0)
                },
                "pwp": {
                    "target": float(targets['pwp_target'] or 0) if targets else 0,
                    "current": float(current['pwp'] or 0) if current else 0,
                    "progress": calc_progress(current['pwp'] if current else 0,
                                            targets['pwp_target'] if targets else 0)
                },
                "transactions": {
                    "target": int(targets['transaction_count_target'] or 0) if targets else 0,
                    "current": int(current['transactions'] or 0) if current else 0,
                    "progress": calc_progress(current['transactions'] if current else 0,
                                            targets['transaction_count_target'] if targets else 0)
                }
            }

            return {"success": True, "data": result, "period": month or period.strftime("%Y-%m")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching targets: {str(e)}")


@app.get("/api/v1/targets/template")
async def download_target_template():
    """Download Excel template for target upload."""
    if not EXCEL_AVAILABLE:
        raise HTTPException(status_code=500, detail="Excel support not available. Install openpyxl.")

    wb = Workbook()
    ws = wb.active
    ws.title = "Targets"

    # Headers
    headers = [
        "staff_id", "year_month", "total_sales", "house_brand",
        "focused_1", "focused_2", "focused_3", "pwp", "clearance", "transactions"
    ]
    for col, header in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=header)

    # Sample row
    sample = ["SM001", 202501, 50000, 5000, 3000, 2000, 1000, 500, 500, 500]
    for col, value in enumerate(sample, 1):
        ws.cell(row=2, column=col, value=value)

    # Save to bytes
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=target_template.xlsx"}
    )


@app.post("/api/v1/targets/upload")
async def upload_targets(
    file: UploadFile = File(...),
    token: str = Query(..., description="Session token")
):
    """Upload targets from Excel file (Operations Manager only)."""
    # Verify session and permissions
    if token not in sessions:
        raise HTTPException(status_code=401, detail="Invalid session")

    user = sessions[token]['user']
    if not user['permissions'].get('can_upload_targets'):
        raise HTTPException(status_code=403, detail="Permission denied. Only Operations Manager can upload targets.")

    if not EXCEL_AVAILABLE:
        raise HTTPException(status_code=500, detail="Excel support not available. Install openpyxl.")

    try:
        # Read Excel file
        contents = await file.read()
        wb = openpyxl.load_workbook(io.BytesIO(contents))
        ws = wb.active

        # Parse rows (skip header)
        rows_processed = 0
        errors = []

        async with pool.acquire() as conn:
            for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
                if not row[0]:  # Skip empty rows
                    continue

                try:
                    staff_id = str(row[0]).strip()
                    year_month = int(row[1])
                    total_sales = float(row[2] or 0)
                    house_brand = float(row[3] or 0)
                    focused_1 = float(row[4] or 0)
                    focused_2 = float(row[5] or 0)
                    focused_3 = float(row[6] or 0)
                    pwp = float(row[7] or 0)
                    clearance = float(row[8] or 0)
                    transactions = int(row[9] or 0)

                    # Upsert target
                    await conn.execute("""
                        INSERT INTO "KPITargets" (
                            salesman_id, year_month, total_sales_target, house_brand_target,
                            focused_item_1_target, focused_item_2_target, focused_item_3_target,
                            pwp_target, stock_clearance_target, transaction_count_target,
                            updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
                        ON CONFLICT (salesman_id, year_month)
                        DO UPDATE SET
                            total_sales_target = EXCLUDED.total_sales_target,
                            house_brand_target = EXCLUDED.house_brand_target,
                            focused_item_1_target = EXCLUDED.focused_item_1_target,
                            focused_item_2_target = EXCLUDED.focused_item_2_target,
                            focused_item_3_target = EXCLUDED.focused_item_3_target,
                            pwp_target = EXCLUDED.pwp_target,
                            stock_clearance_target = EXCLUDED.stock_clearance_target,
                            transaction_count_target = EXCLUDED.transaction_count_target,
                            updated_at = NOW()
                    """, staff_id, year_month, total_sales, house_brand, focused_1,
                        focused_2, focused_3, pwp, clearance, transactions)

                    rows_processed += 1

                except Exception as e:
                    errors.append(f"Row {row_idx}: {str(e)}")

        return {
            "success": True,
            "rows_processed": rows_processed,
            "errors": errors if errors else None
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# ============================================================================
# Commission Calculation Endpoints
# ============================================================================

@app.get("/api/v1/commission/me")
async def get_my_commission(
    staff_id: str = Query(..., description="Staff ID"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None)
):
    """Calculate commission earned from actual sales."""
    if not start_date:
        start_date = date.today().replace(day=1)
    if not end_date:
        end_date = date.today()

    try:
        async with pool.acquire() as conn:
            # Calculate commission from transactions
            # Commission = ItemAmount * CommissionByPercentStockPrice1 / 100
            result = await conn.fetchrow("""
                SELECT
                    COUNT(DISTINCT d."DocumentNo") as transaction_count,
                    SUM(d."ItemAmount") as total_sales,
                    SUM(d."ItemAmount" * COALESCE(s."CommissionByPercentStockPrice1", 0) / 100) as commission
                FROM "AcCSD" d
                JOIN "AcCSM" c ON d."DocumentNo" = c."DocumentNo"
                LEFT JOIN "AcStockCompany" s
                    ON d."AcStockID" = s."AcStockID"
                    AND d."AcStockUOMID" = s."AcStockUOMID"
                WHERE d."AcSalesmanID" = $1
                  AND c."DocumentDate" BETWEEN $2 AND $3
                  AND d."ItemAmount" > 0
            """, staff_id, start_date, end_date)

            # Get today's commission
            today = date.today()
            today_result = await conn.fetchrow("""
                SELECT
                    SUM(d."ItemAmount" * COALESCE(s."CommissionByPercentStockPrice1", 0) / 100) as commission
                FROM "AcCSD" d
                JOIN "AcCSM" c ON d."DocumentNo" = c."DocumentNo"
                LEFT JOIN "AcStockCompany" s
                    ON d."AcStockID" = s."AcStockID"
                    AND d."AcStockUOMID" = s."AcStockUOMID"
                WHERE d."AcSalesmanID" = $1
                  AND c."DocumentDate" = $2
                  AND d."ItemAmount" > 0
            """, staff_id, today)

            # Get commission breakdown by product category
            breakdown = await conn.fetch("""
                SELECT
                    COALESCE(s."AcStockUDGroup1ID", 'OTHER') as category,
                    SUM(d."ItemAmount") as sales,
                    SUM(d."ItemAmount" * COALESCE(s."CommissionByPercentStockPrice1", 0) / 100) as commission
                FROM "AcCSD" d
                JOIN "AcCSM" c ON d."DocumentNo" = c."DocumentNo"
                LEFT JOIN "AcStockCompany" s
                    ON d."AcStockID" = s."AcStockID"
                    AND d."AcStockUOMID" = s."AcStockUOMID"
                WHERE d."AcSalesmanID" = $1
                  AND c."DocumentDate" BETWEEN $2 AND $3
                  AND d."ItemAmount" > 0
                GROUP BY s."AcStockUDGroup1ID"
                ORDER BY commission DESC
                LIMIT 10
            """, staff_id, start_date, end_date)

            return {
                "success": True,
                "data": {
                    "period": {
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat()
                    },
                    "summary": {
                        "total_sales": float(result['total_sales'] or 0),
                        "commission_earned": float(result['commission'] or 0),
                        "transaction_count": int(result['transaction_count'] or 0)
                    },
                    "today": {
                        "commission_earned": float(today_result['commission'] or 0) if today_result else 0
                    },
                    "breakdown": [
                        {
                            "category": row['category'],
                            "sales": float(row['sales'] or 0),
                            "commission": float(row['commission'] or 0)
                        }
                        for row in breakdown
                    ]
                }
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating commission: {str(e)}")


# ============================================================================
# Notification Endpoints
# ============================================================================

@app.get("/api/v1/notifications")
async def get_notifications(
    staff_id: str = Query(..., description="Staff ID"),
    limit: int = Query(20, ge=1, le=100),
    unread_only: bool = Query(False)
):
    """Get notifications for a staff member."""
    try:
        async with pool.acquire() as conn:
            # Check if notifications table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'kpi' AND table_name = 'notifications'
                )
            """)

            if not table_exists:
                return {"success": True, "data": [], "message": "Notifications not yet configured"}

            query = """
                SELECT id, type, title, message, data, is_read, created_at
                FROM kpi.notifications
                WHERE staff_id = $1
            """
            if unread_only:
                query += " AND is_read = FALSE"
            query += " ORDER BY created_at DESC LIMIT $2"

            rows = await conn.fetch(query, staff_id, limit)

            return {
                "success": True,
                "data": [
                    {
                        "id": row['id'],
                        "type": row['type'],
                        "title": row['title'],
                        "message": row['message'],
                        "data": json.loads(row['data']) if row['data'] else None,
                        "is_read": row['is_read'],
                        "created_at": row['created_at'].isoformat()
                    }
                    for row in rows
                ]
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching notifications: {str(e)}")


@app.post("/api/v1/notifications/{notification_id}/read")
async def mark_notification_read(notification_id: int):
    """Mark a notification as read."""
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE kpi.notifications SET is_read = TRUE WHERE id = $1
            """, notification_id)
            return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.post("/api/v1/notifications/read-all")
async def mark_all_notifications_read(staff_id: str = Query(...)):
    """Mark all notifications as read for a staff member."""
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE kpi.notifications SET is_read = TRUE WHERE staff_id = $1
            """, staff_id)
            return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ============================================================================
# Staff List Endpoint (for dropdowns)
# ============================================================================

@app.get("/api/v1/staff/list")
async def get_staff_list(
    outlet_id: Optional[str] = Query(None, description="Filter by outlet"),
    limit: int = Query(100, ge=1, le=500)
):
    """Get list of active staff members."""
    try:
        async with pool.acquire() as conn:
            if outlet_id:
                # Get staff who have sales at this outlet (from materialized view)
                rows = await conn.fetch("""
                    SELECT DISTINCT
                        s."AcSalesmanID" as staff_id,
                        s."AcSalesmanName" as name,
                        $1 as outlet_id
                    FROM "AcSalesman" s
                    INNER JOIN analytics.mv_staff_daily_kpi k ON s."AcSalesmanID" = k.staff_id
                    WHERE k.outlet_id = $1
                    ORDER BY s."AcSalesmanName"
                    LIMIT $2
                """, outlet_id, limit)
            else:
                # Get all staff with their most recent outlet
                rows = await conn.fetch("""
                    SELECT DISTINCT ON (s."AcSalesmanID")
                        s."AcSalesmanID" as staff_id,
                        s."AcSalesmanName" as name,
                        k.outlet_id
                    FROM "AcSalesman" s
                    LEFT JOIN analytics.mv_staff_daily_kpi k ON s."AcSalesmanID" = k.staff_id
                    ORDER BY s."AcSalesmanID", k.sale_date DESC NULLS LAST
                    LIMIT $1
                """, limit)

            return {
                "success": True,
                "data": [
                    {
                        "staff_id": row['staff_id'],
                        "name": row['name'],
                        "outlet_id": row['outlet_id']
                    }
                    for row in rows
                ]
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ============================================================================
# Push Notification Endpoints
# ============================================================================

class PushSubscription(BaseModel):
    staff_id: str
    subscription: dict  # Contains endpoint, keys.p256dh, keys.auth


@app.post("/api/v1/push/subscribe")
async def subscribe_to_push(data: PushSubscription):
    """Subscribe to push notifications."""
    try:
        sub = data.subscription
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO kpi.push_subscriptions (staff_id, endpoint, p256dh, auth, user_agent)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (staff_id, endpoint)
                DO UPDATE SET p256dh = $3, auth = $4, last_used_at = NOW()
            """,
                data.staff_id,
                sub.get('endpoint'),
                sub.get('keys', {}).get('p256dh'),
                sub.get('keys', {}).get('auth'),
                None  # user_agent can be added later
            )
            return {"success": True, "message": "Subscribed to push notifications"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Subscription failed: {str(e)}")


class UnsubscribeRequest(BaseModel):
    staff_id: str
    endpoint: str


@app.post("/api/v1/push/unsubscribe")
async def unsubscribe_from_push(data: UnsubscribeRequest):
    """Unsubscribe from push notifications."""
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM kpi.push_subscriptions
                WHERE staff_id = $1 AND endpoint = $2
            """, data.staff_id, data.endpoint)
            return {"success": True, "message": "Unsubscribed from push notifications"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unsubscribe failed: {str(e)}")


# ============================================================================
# Debug Endpoints
# ============================================================================

@app.get("/api/v1/debug/sales-breakdown")
async def debug_sales_breakdown(
    outlet_id: str = Query(..., description="Outlet ID"),
    start_date: date = Query(..., description="Start date"),
    end_date: date = Query(..., description="End date")
):
    """Debug endpoint to compare cash vs invoice sales from raw tables."""
    try:
        async with pool.acquire() as conn:
            # Cash Sales from AcCSD + AcCSM
            cash_sales = await conn.fetchrow("""
                SELECT
                    COUNT(DISTINCT m."DocumentNo") as transactions,
                    SUM(d."ItemTotal") as total
                FROM "AcCSD" d
                INNER JOIN "AcCSM" m ON d."DocumentNo" = m."DocumentNo"
                WHERE m."AcLocationID" = $1
                  AND m."DocumentDate"::date BETWEEN $2 AND $3
            """, outlet_id, start_date, end_date)

            # Invoice Sales from AcCusInvoiceD + AcCusInvoiceM
            invoice_sales = await conn.fetchrow("""
                SELECT
                    COUNT(DISTINCT m."AcCusInvoiceMID") as transactions,
                    SUM(d."ItemTotalPrice") as total
                FROM "AcCusInvoiceD" d
                INNER JOIN "AcCusInvoiceM" m ON d."AcCusInvoiceMID" = m."AcCusInvoiceMID"
                WHERE m."AcLocationID" = $1
                  AND m."DocumentDate"::date BETWEEN $2 AND $3
            """, outlet_id, start_date, end_date)

            # Materialized view total (for comparison)
            mv_total = await conn.fetchrow("""
                SELECT
                    SUM(transactions) as transactions,
                    SUM(total_sales) as total
                FROM analytics.mv_outlet_daily_kpi
                WHERE outlet_id = $1
                  AND sale_date BETWEEN $2 AND $3
            """, outlet_id, start_date, end_date)

            cash_total = float(cash_sales['total'] or 0)
            invoice_total = float(invoice_sales['total'] or 0)
            combined = cash_total + invoice_total

            return {
                "success": True,
                "outlet_id": outlet_id,
                "period": {"start": start_date.isoformat(), "end": end_date.isoformat()},
                "breakdown": {
                    "cash_sales": {
                        "transactions": int(cash_sales['transactions'] or 0),
                        "total": round(cash_total, 2)
                    },
                    "invoice_sales": {
                        "transactions": int(invoice_sales['transactions'] or 0),
                        "total": round(invoice_total, 2)
                    },
                    "combined_total": round(combined, 2),
                    "materialized_view_total": round(float(mv_total['total'] or 0), 2)
                }
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
