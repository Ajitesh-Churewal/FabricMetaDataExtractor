# =============================================================================
# FABRIC METADATA EXTRACTOR
# =============================================================================
# Description : Extracts metadata from Microsoft Fabric workspaces and assets
#               using Fabric REST APIs and Power BI REST APIs.
# Target      : Microsoft Fabric Notebook
# Auth        : notebookutils.credentials.getToken() — runs as notebook user
# Lakehouse   : Auto-creates "MetaDataManagement" in the current workspace
# Write mode  : Overwrite (full refresh on every run)
# Refresh Hist: Last 30 days
#
# Tables created:
#   meta_workspaces          — workspace registry
#   meta_workspace_users     — users & roles per workspace
#   meta_workspace_items     — unified item catalog (all types)
#   meta_semantic_models     — dataset/semantic model details
#   meta_datasources         — data source connections for Semantic Models,
#                              Dataflows (Gen1 + Gen2), and Lakehouse Shortcuts
#   meta_refresh_schedules   — scheduled refresh configuration
#   meta_refresh_history     — refresh run history (last 30 days)
#   meta_reports             — Power BI report details
#   meta_dataflows           — dataflow metadata
#   meta_other_assets        — lakehouses, notebooks, warehouses, pipelines, etc.
#   meta_access_errors       — items skipped due to insufficient permissions
#   meta_power_queries       — M / SQL query expressions per table partition,
#                              extracted via DAX INFO functions (no admin needed,
#                              Build permission on dataset sufficient).
#                              Works for classic .pbix imports, DirectQuery,
#                              and Fabric-native semantic models.
#
# meta_power_queries.partition_type values:
#   M            — Power Query / M expression (Import mode)
#   SQL          — Native SQL query (DirectQuery)
#   DAX          — Calculated table (DAX expression)
#   Push         — Push / streaming dataset partition
#   Unknown      — Type could not be determined
#
# meta_datasources.source_item_type values:
#   SemanticModel   — dataset connected data sources
#   Dataflow        — Power BI Dataflow Gen1 data sources
#   DataflowGen2    — Fabric Dataflow Gen2 data sources
#   Lakehouse       — OneLake shortcuts (external connections)
# =============================================================================

# ── IMPORTS ──────────────────────────────────────────────────────────────────
import requests
import json
import uuid
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple, Any

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
LAKEHOUSE_NAME        = "MetaDataManagement"
REFRESH_HISTORY_DAYS  = 30
FABRIC_API_BASE       = "https://api.fabric.microsoft.com/v1"
PBI_API_BASE          = "https://api.powerbi.com/v1.0/myorg"

# ── SCOPE PARAMETER ───────────────────────────────────────────────────────────
# "current"  → extract metadata only from the workspace this notebook runs in
#              (safe for testing — fast, no cross-workspace permissions needed)
# "all"      → extract metadata from every workspace your account has access to
#              (use in production)
EXTRACTION_SCOPE = "current"   # ← change to "all" for production

# Asset types routed to meta_other_assets
OTHER_ASSET_TYPES = {
    "Lakehouse", "Notebook", "Warehouse", "DataPipeline",
    "MLModel", "MLExperiment", "SparkJobDefinition",
    "KQLDatabase", "KQLQueryset", "Eventstream", "Reflex",
    "Environment", "MirroredDatabase", "GraphQLApi"
}

# Fabric item types that represent Dataflow Gen2
DATAFLOW_GEN2_TYPES = {"DataflowsGen2", "DataflowGen2", "Dataflow Gen2"}

# ── GLOBAL STATE ──────────────────────────────────────────────────────────────
access_errors: List[Dict] = []
extraction_ts = datetime.now(timezone.utc)

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()


# =============================================================================
# SECTION 1 — AUTHENTICATION
# =============================================================================

def get_fabric_token() -> str:
    """Returns a bearer token scoped to the Fabric REST API."""
    return notebookutils.credentials.getToken("https://api.fabric.microsoft.com")


def get_pbi_token() -> str:
    """Returns a bearer token scoped to the Power BI REST API."""
    return notebookutils.credentials.getToken("https://analysis.windows.net/powerbi/api")


def fabric_headers() -> Dict:
    return {
        "Authorization": f"Bearer {get_fabric_token()}",
        "Content-Type":  "application/json"
    }


def pbi_headers() -> Dict:
    return {
        "Authorization": f"Bearer {get_pbi_token()}",
        "Content-Type":  "application/json"
    }


# =============================================================================
# SECTION 2 — API HELPERS
# =============================================================================

def api_get(
    url:          str,
    headers:      Dict,
    workspace_id: str = None,
    item_id:      str = None,
    item_type:    str = None,
    item_name:    str = None,
    operation:    str = None,
    max_retries:  int = 3
) -> Optional[Dict]:
    """
    HTTP GET with error handling and retry on 429.
    - 200      → returns parsed JSON
    - 401/403  → logs to meta_access_errors, returns None  (least-privilege)
    - 404      → silently returns None
    - 429      → respects Retry-After header, retries up to max_retries times
    - Other    → logs to meta_access_errors, returns None
    """
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=60)

            if resp.status_code == 200:
                return resp.json()

            elif resp.status_code in (401, 403):
                _log_access_error(
                    workspace_id=workspace_id, item_id=item_id,
                    item_type=item_type, item_name=item_name,
                    operation=operation or url,
                    http_status_code=resp.status_code,
                    error_body=resp.text,
                    requires_admin=(resp.status_code == 403)
                )
                return None

            elif resp.status_code == 404:
                return None  # Resource doesn't exist; skip silently

            elif resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 10))
                print(f"  [WARN] Rate limited on '{operation or url}'. "
                      f"Waiting {retry_after}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(retry_after)
                # Refresh token before retry
                headers = fabric_headers() if "fabric.microsoft.com" in url else pbi_headers()
                continue

            else:
                _log_access_error(
                    workspace_id=workspace_id, item_id=item_id,
                    item_type=item_type, item_name=item_name,
                    operation=operation or url,
                    http_status_code=resp.status_code,
                    error_body=resp.text,
                    requires_admin=False
                )
                return None

        except requests.exceptions.Timeout:
            print(f"  [WARN] Timeout on '{operation or url}' (attempt {attempt + 1}/{max_retries})")
            if attempt == max_retries - 1:
                _log_access_error(
                    workspace_id=workspace_id, item_id=item_id,
                    item_type=item_type, item_name=item_name,
                    operation=operation or url,
                    http_status_code=-1,
                    error_body="Request timed out after 60s",
                    requires_admin=False
                )
        except Exception as ex:
            _log_access_error(
                workspace_id=workspace_id, item_id=item_id,
                item_type=item_type, item_name=item_name,
                operation=operation or url,
                http_status_code=-1,
                error_body=str(ex),
                requires_admin=False
            )
            return None

    return None


def paginate(
    url:          str,
    headers:      Dict,
    value_key:    str = "value",
    workspace_id: str = None,
    item_id:      str = None,
    item_type:    str = None,
    item_name:    str = None,
    operation:    str = None
) -> List[Dict]:
    """
    Fetches all pages from a paginated endpoint.
    Follows nextLink / @odata.nextLink until exhausted.
    """
    results:  List[Dict]    = []
    next_url: Optional[str] = url

    while next_url:
        data = api_get(
            next_url, headers,
            workspace_id=workspace_id, item_id=item_id,
            item_type=item_type, item_name=item_name,
            operation=operation
        )
        if data is None:
            break
        results.extend(data.get(value_key, []))
        next_url = data.get("nextLink") or data.get("@odata.nextLink")

    return results


def _log_access_error(
    workspace_id, item_id, item_type, item_name,
    operation, http_status_code, error_body, requires_admin
):
    """Parses an error response and appends it to the global access_errors list."""
    try:
        body = json.loads(error_body) if isinstance(error_body, str) else (error_body or {})
        error_code    = body.get("error", {}).get("code",    "")
        error_message = body.get("error", {}).get("message", str(error_body))
    except Exception:
        error_code    = ""
        error_message = str(error_body)

    access_errors.append({
        "error_id":         str(uuid.uuid4()),
        "workspace_id":     workspace_id  or "",
        "workspace_name":   "",            # enriched after workspace list is built
        "item_id":          item_id        or "",
        "item_type":        item_type      or "",
        "item_name":        item_name      or "",
        "operation":        operation      or "",
        "http_status_code": http_status_code,
        "error_code":       error_code,
        "error_message":    error_message,
        "requires_admin":   requires_admin,
        "extracted_at":     extraction_ts
    })
    print(f"  [ACCESS ERROR] HTTP {http_status_code} | {operation} | {error_message[:120]}")


# =============================================================================
# SECTION 3 — LAKEHOUSE MANAGEMENT
# =============================================================================

def get_or_create_lakehouse(workspace_id: str) -> Tuple[str, str]:
    """
    Finds MetaDataManagement lakehouse in workspace_id; creates it if absent.
    Handles Fabric long-running operation (202) with polling.
    Returns (lakehouse_id, abfs_tables_root_path).
    """
    headers = fabric_headers()

    # ── 1. Check for existing lakehouse ───────────────────────────────────────
    items = paginate(
        f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items?type=Lakehouse",
        headers,
        workspace_id=workspace_id,
        operation="ListLakehouses"
    )
    for item in items:
        if item.get("displayName") == LAKEHOUSE_NAME:
            lh_id = item["id"]
            print(f"[INFO] Found existing lakehouse '{LAKEHOUSE_NAME}' → {lh_id}")
            return lh_id, _abfs_path(workspace_id, lh_id)

    # ── 2. Create new lakehouse ────────────────────────────────────────────────
    print(f"[INFO] Lakehouse '{LAKEHOUSE_NAME}' not found. Creating...")
    resp = requests.post(
        f"{FABRIC_API_BASE}/workspaces/{workspace_id}/lakehouses",
        headers=headers,
        json={
            "displayName": LAKEHOUSE_NAME,
            "description": "Auto-created by Fabric Metadata Extractor"
        },
        timeout=60
    )

    if resp.status_code in (200, 201):
        lh_id = resp.json()["id"]

    elif resp.status_code == 202:
        op_url = resp.headers.get("Location") or resp.headers.get("x-ms-operation-id", "")
        if not op_url:
            raise RuntimeError("202 received but no Location header present.")
        print("[INFO] Lakehouse creation is async — polling for completion...")
        lh_id = _poll_lro(op_url, headers, timeout_seconds=180)

    else:
        raise RuntimeError(
            f"Failed to create lakehouse '{LAKEHOUSE_NAME}': "
            f"HTTP {resp.status_code} — {resp.text}"
        )

    print(f"[INFO] Lakehouse '{LAKEHOUSE_NAME}' ready → {lh_id}")
    return lh_id, _abfs_path(workspace_id, lh_id)


def _poll_lro(op_url: str, headers: Dict, timeout_seconds: int = 180) -> str:
    """Polls a Fabric LRO URL until Succeeded/Failed. Returns the new item ID."""
    deadline = time.time() + timeout_seconds
    interval = 5

    while time.time() < deadline:
        time.sleep(interval)
        resp = requests.get(op_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"LRO poll returned HTTP {resp.status_code} — {resp.text}")

        data   = resp.json()
        status = data.get("status", "")

        if status == "Succeeded":
            lh_id = (
                data.get("result", {}).get("id")
                or data.get("createdItemId")
            )
            if not lh_id:
                raise RuntimeError(f"LRO succeeded but no item ID found in response: {data}")
            return lh_id

        elif status == "Failed":
            raise RuntimeError(f"Lakehouse creation LRO failed: {data}")

        print(f"  [LRO] Status: {status} — retrying in {interval}s...")

    raise TimeoutError(f"Lakehouse LRO did not complete within {timeout_seconds}s")


def _abfs_path(workspace_id: str, lakehouse_id: str) -> str:
    return (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com"
        f"/{lakehouse_id}/Tables"
    )


def write_table(rows: List[Dict], table_name: str, lakehouse_id: str, workspace_id: str):
    """
    Converts list-of-dicts → Spark DataFrame and writes as an overwrite Delta table
    inside the MetaDataManagement lakehouse.
    """
    if not rows:
        print(f"  [SKIP] {table_name} — 0 rows, nothing to write")
        return

    df   = spark.createDataFrame(rows)
    path = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com"
        f"/{lakehouse_id}/Tables/{table_name}"
    )
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(path))

    print(f"  [OK] {table_name:40s} → {df.count():>6,} rows")


# =============================================================================
# SECTION 4 — EXTRACT: WORKSPACES
# =============================================================================

def extract_workspaces() -> List[Dict]:
    print("\n[STEP 1/11] Extracting workspaces...")
    raw = paginate(
        f"{FABRIC_API_BASE}/workspaces",
        fabric_headers(),
        operation="ListWorkspaces"
    )
    rows = [{
        "workspace_id":             ws.get("id",          ""),
        "workspace_name":           ws.get("displayName", ""),
        "workspace_type":           ws.get("type",        ""),
        "state":                    ws.get("state",       ""),
        "capacity_id":              ws.get("capacityId",  "") or "",
        "description":              ws.get("description", "") or "",
        "is_on_dedicated_capacity": bool(ws.get("capacityId")),
        "extracted_at":             extraction_ts
    } for ws in raw]

    print(f"  → {len(rows)} workspaces")
    return rows


# =============================================================================
# SECTION 5 — EXTRACT: WORKSPACE USERS / ROLE ASSIGNMENTS
# =============================================================================

def extract_workspace_users(workspace_ids: List[str]) -> List[Dict]:
    print("\n[STEP 2/11] Extracting workspace role assignments...")
    headers = fabric_headers()
    rows    = []

    for ws_id in workspace_ids:
        for a in paginate(
            f"{FABRIC_API_BASE}/workspaces/{ws_id}/roleAssignments",
            headers,
            workspace_id=ws_id,
            operation="GetWorkspaceRoleAssignments"
        ):
            p  = a.get("principal", {})
            email = (
                p.get("userDetails",           {}).get("userPrincipalName", "")
                or p.get("servicePrincipalDetails", {}).get("aadAppId",    "")
                or p.get("groupDetails",        {}).get("email",           "")
                or ""
            )
            rows.append({
                "id":             f"{ws_id}_{p.get('id', str(uuid.uuid4()))}",
                "workspace_id":   ws_id,
                "user_email":     email,
                "display_name":   p.get("displayName", ""),
                "principal_type": p.get("type",        ""),
                "workspace_role": a.get("role",        ""),
                "extracted_at":   extraction_ts
            })

    print(f"  → {len(rows)} role assignments")
    return rows


# =============================================================================
# SECTION 6 — EXTRACT: WORKSPACE ITEMS (unified catalog)
# =============================================================================

def extract_workspace_items(workspace_ids: List[str]) -> List[Dict]:
    print("\n[STEP 3/11] Extracting workspace items...")
    headers = fabric_headers()
    rows    = []

    for ws_id in workspace_ids:
        for item in paginate(
            f"{FABRIC_API_BASE}/workspaces/{ws_id}/items",
            headers,
            workspace_id=ws_id,
            operation="ListWorkspaceItems"
        ):
            created_by = (
                item.get("createdBy", {})
                    .get("userIdentity", {})
                    .get("userPrincipalName", "")
            )
            rows.append({
                "item_id":            item.get("id",          ""),
                "workspace_id":       ws_id,
                "item_name":          item.get("displayName", ""),
                "item_type":          item.get("type",        ""),
                "description":        item.get("description", "") or "",
                "created_date":       _parse_ts(
                                          item.get("createdDate")
                                          or item.get("createdDateTime")
                                      ),
                "last_modified_date": _parse_ts(
                                          item.get("lastUpdatedDate")
                                          or item.get("lastModifiedDateTime")
                                      ),
                "created_by_email":   created_by,
                "extracted_at":       extraction_ts
            })

    print(f"  → {len(rows)} items")
    return rows


# =============================================================================
# SECTION 7 — EXTRACT: SEMANTIC MODELS (DATASETS)
# =============================================================================

def extract_semantic_models(workspace_ids: List[str]) -> List[Dict]:
    print("\n[STEP 4/11] Extracting semantic models...")
    headers = pbi_headers()
    rows    = []

    for ws_id in workspace_ids:
        for ds in paginate(
            f"{PBI_API_BASE}/groups/{ws_id}/datasets",
            headers,
            workspace_id=ws_id,
            operation="ListDatasets"
        ):
            rows.append({
                "model_id":              ds.get("id",                  ""),
                "workspace_id":          ws_id,
                "model_name":            ds.get("name",                ""),
                "configured_by":         ds.get("configuredBy",        "") or "",
                "target_storage_mode":   ds.get("targetStorageMode",   "") or "",
                "is_refreshable":        bool(ds.get("isRefreshable",  False)),
                "content_provider_type": ds.get("contentProviderType", "") or "",
                "created_date":          _parse_ts(ds.get("createdDate")),
                "modified_date":         _parse_ts(ds.get("LastRefreshTime")),
                "extracted_at":          extraction_ts
            })

    print(f"  → {len(rows)} semantic models")
    return rows


# =============================================================================
# SECTION 8 — EXTRACT: DATASOURCES
# =============================================================================
# Sources covered:
#   [A] Semantic Model datasources  (Power BI API)
#   [B] Dataflow Gen1 datasources   (Power BI API)
#   [C] Dataflow Gen2 datasources   (Power BI API — same endpoint, different item type)
#   [D] Lakehouse shortcuts         (Fabric API — external OneLake connections)
#
# Unified schema — source_item_type distinguishes origin:
#   datasource_id, source_item_id, source_item_type, source_item_name,
#   workspace_id, datasource_type, server, database, url,
#   gateway_id, gateway_name, credential_type, credential_username,
#   shortcut_name, shortcut_path, extracted_at
# =============================================================================

def _build_datasource_row(
    ds:               Dict,
    source_item_id:   str,
    source_item_type: str,
    source_item_name: str,
    workspace_id:     str
) -> Dict:
    """Normalises a Power BI datasource API record into the unified schema."""
    conn = ds.get("connectionDetails", {}) or {}
    cred = ds.get("credentialDetails",  {}) or {}
    return {
        "datasource_id":       ds.get("datasourceId", str(uuid.uuid4())),
        "source_item_id":      source_item_id,
        "source_item_type":    source_item_type,
        "source_item_name":    source_item_name,
        "workspace_id":        workspace_id,
        "datasource_type":     ds.get("datasourceType", ""),
        "server":              conn.get("server",   "") or conn.get("url", "") or "",
        "database":            conn.get("database", "") or "",
        "url":                 conn.get("url",      "") or "",
        "gateway_id":          ds.get("gatewayId",  "") or "",
        "gateway_name":        "",   # Requires Fabric Admin gateway call
        "credential_type":     cred.get("credentialType", "") or "",
        "credential_username": cred.get("username",        "") or "",
        "shortcut_name":       "",   # Populated only for Lakehouse shortcuts
        "shortcut_path":       "",   # Populated only for Lakehouse shortcuts
        "extracted_at":        extraction_ts
    }


def _build_shortcut_row(
    shortcut:         Dict,
    lakehouse_id:     str,
    lakehouse_name:   str,
    workspace_id:     str
) -> Dict:
    """Normalises a Fabric Lakehouse shortcut into the unified datasource schema."""
    target     = shortcut.get("target", {}) or {}
    target_type = target.get("type", "")

    # Each shortcut type stores connection info in a differently-named sub-object
    # e.g. target.adlsGen2, target.s3, target.oneLake, target.googleCloudStorage
    conn_detail = target.get(target_type, {}) or {}

    server = (
        conn_detail.get("location", "")
        or conn_detail.get("url",      "")
        or conn_detail.get("endpoint", "")
        or ""
    )
    database = (
        conn_detail.get("subpath",    "")
        or conn_detail.get("bucket",   "")
        or conn_detail.get("path",     "")
        or ""
    )

    return {
        "datasource_id":       str(uuid.uuid4()),
        "source_item_id":      lakehouse_id,
        "source_item_type":    "Lakehouse",
        "source_item_name":    lakehouse_name,
        "workspace_id":        workspace_id,
        "datasource_type":     target_type,          # adlsGen2 / s3 / oneLake / etc.
        "server":              server,
        "database":            database,
        "url":                 conn_detail.get("url", "") or server,
        "gateway_id":          conn_detail.get("connectionId", "") or "",
        "gateway_name":        "",
        "credential_type":     "",   # Shortcuts use workspace identity / MSI
        "credential_username": "",
        "shortcut_name":       shortcut.get("name",    ""),
        "shortcut_path":       shortcut.get("path",    ""),
        "extracted_at":        extraction_ts
    }


def extract_datasources(
    workspace_ids: List[str],
    model_rows:    List[Dict],
    dataflow_rows: List[Dict],
    item_rows:     List[Dict]          # workspace items catalog (for Gen2 & Lakehouse)
) -> List[Dict]:
    """
    Extracts data sources from:
      [A] Semantic Models
      [B] Dataflows Gen1
      [C] Dataflows Gen2
      [D] Lakehouses (via shortcuts)
    Returns a unified list of datasource rows.
    """
    print("\n[STEP 5/11] Extracting datasources (Semantic Models / Dataflows / Lakehouses)...")
    pbi_hdr    = pbi_headers()
    fab_hdr    = fabric_headers()
    rows: List[Dict] = []

    # ── Build lookup maps ─────────────────────────────────────────────────────
    # model_id → model_name
    model_name_map = {m["model_id"]: m["model_name"] for m in model_rows}

    # dataflow objectId → dataflow_name, workspace_id
    df_map = {d["dataflow_id"]: d for d in dataflow_rows}

    # workspace_id → list of items by type
    items_by_ws: Dict[str, List[Dict]] = {}
    for item in item_rows:
        items_by_ws.setdefault(item["workspace_id"], []).append(item)

    # ── [A] Semantic Model datasources ────────────────────────────────────────
    print("  [A] Semantic Model datasources...")
    model_by_ws: Dict[str, List[str]] = {}
    for m in model_rows:
        model_by_ws.setdefault(m["workspace_id"], []).append(m["model_id"])

    for ws_id in workspace_ids:
        for model_id in model_by_ws.get(ws_id, []):
            data = api_get(
                f"{PBI_API_BASE}/groups/{ws_id}/datasets/{model_id}/datasources",
                pbi_hdr,
                workspace_id=ws_id, item_id=model_id,
                item_type="SemanticModel",
                item_name=model_name_map.get(model_id, ""),
                operation="GetSemanticModelDatasources"
            )
            for ds in (data or {}).get("value", []):
                rows.append(_build_datasource_row(
                    ds, model_id, "SemanticModel",
                    model_name_map.get(model_id, ""), ws_id
                ))

    # ── [B] Dataflow Gen1 datasources ─────────────────────────────────────────
    print("  [B] Dataflow Gen1 datasources...")
    for df in dataflow_rows:
        ws_id  = df["workspace_id"]
        df_id  = df["dataflow_id"]
        df_name = df["dataflow_name"]

        data = api_get(
            f"{PBI_API_BASE}/groups/{ws_id}/dataflows/{df_id}/datasources",
            pbi_hdr,
            workspace_id=ws_id, item_id=df_id,
            item_type="Dataflow",
            item_name=df_name,
            operation="GetDataflowDatasources"
        )
        for ds in (data or {}).get("value", []):
            rows.append(_build_datasource_row(
                ds, df_id, "Dataflow", df_name, ws_id
            ))

    # ── [C] Dataflow Gen2 datasources ─────────────────────────────────────────
    # Gen2 Dataflows appear in workspace items with type in DATAFLOW_GEN2_TYPES.
    # Their datasources are accessed via the same Power BI endpoint using the
    # item's Fabric ID (which mirrors the dataset ID in Power BI for Gen2 flows).
    print("  [C] Dataflow Gen2 datasources...")
    for ws_id in workspace_ids:
        gen2_items = [
            i for i in items_by_ws.get(ws_id, [])
            if i["item_type"] in DATAFLOW_GEN2_TYPES
        ]
        for item in gen2_items:
            item_id   = item["item_id"]
            item_name = item["item_name"]

            data = api_get(
                f"{PBI_API_BASE}/groups/{ws_id}/dataflows/{item_id}/datasources",
                pbi_hdr,
                workspace_id=ws_id, item_id=item_id,
                item_type="DataflowGen2",
                item_name=item_name,
                operation="GetDataflowGen2Datasources"
            )
            for ds in (data or {}).get("value", []):
                rows.append(_build_datasource_row(
                    ds, item_id, "DataflowGen2", item_name, ws_id
                ))

    # ── [D] Lakehouse shortcuts ────────────────────────────────────────────────
    # Shortcuts are external connections mounted inside a Lakehouse (e.g. ADLS Gen2,
    # Amazon S3, OneLake cross-workspace). Each shortcut is a logical datasource.
    print("  [D] Lakehouse shortcuts...")
    for ws_id in workspace_ids:
        lakehouse_items = [
            i for i in items_by_ws.get(ws_id, [])
            if i["item_type"] == "Lakehouse"
        ]
        for lh in lakehouse_items:
            lh_id   = lh["item_id"]
            lh_name = lh["item_name"]

            shortcuts = paginate(
                f"{FABRIC_API_BASE}/workspaces/{ws_id}/lakehouses/{lh_id}/shortcuts",
                fab_hdr,
                workspace_id=ws_id, item_id=lh_id,
                item_type="Lakehouse",
                item_name=lh_name,
                operation="GetLakehouseShortcuts"
            )
            for sc in shortcuts:
                rows.append(_build_shortcut_row(sc, lh_id, lh_name, ws_id))

    print(f"  → {len(rows)} datasource entries total")
    return rows


# =============================================================================
# SECTION 9 — EXTRACT: REFRESH SCHEDULES
# =============================================================================

def extract_refresh_schedules(workspace_ids: List[str], model_rows: List[Dict]) -> List[Dict]:
    print("\n[STEP 6/11] Extracting refresh schedules...")
    headers = pbi_headers()
    rows    = []

    for m in model_rows:
        if not m.get("is_refreshable"):
            continue
        ws_id    = m["workspace_id"]
        model_id = m["model_id"]

        data = api_get(
            f"{PBI_API_BASE}/groups/{ws_id}/datasets/{model_id}/refreshSchedule",
            headers,
            workspace_id=ws_id, item_id=model_id,
            item_type="SemanticModel",
            item_name=m.get("model_name", ""),
            operation="GetRefreshSchedule"
        )
        if not data:
            continue

        rows.append({
            "schedule_id":        f"{model_id}_{ws_id}",
            "model_id":           model_id,
            "workspace_id":       ws_id,
            "is_enabled":         bool(data.get("enabled",                    False)),
            "notify_option":      data.get("notifyOption",                    "") or "",
            "notification_email": data.get("notificationGroupMailAddress",    "") or "",
            "days":               ",".join(data.get("days",  [])),
            "times":              ",".join(data.get("times", [])),
            "timezone":           data.get("localTimeZoneId",                 "UTC") or "UTC",
            "extracted_at":       extraction_ts
        })

    print(f"  → {len(rows)} refresh schedules")
    return rows


# =============================================================================
# SECTION 10 — EXTRACT: REFRESH HISTORY  (last 30 days)
# =============================================================================

def extract_refresh_history(workspace_ids: List[str], model_rows: List[Dict]) -> List[Dict]:
    print(f"\n[STEP 7/11] Extracting refresh history (last {REFRESH_HISTORY_DAYS} days)...")
    headers = pbi_headers()
    rows    = []
    cutoff  = datetime.now(timezone.utc) - timedelta(days=REFRESH_HISTORY_DAYS)

    for m in model_rows:
        if not m.get("is_refreshable"):
            continue
        ws_id    = m["workspace_id"]
        model_id = m["model_id"]

        data = api_get(
            f"{PBI_API_BASE}/groups/{ws_id}/datasets/{model_id}/refreshes?$top=50",
            headers,
            workspace_id=ws_id, item_id=model_id,
            item_type="SemanticModel",
            item_name=m.get("model_name", ""),
            operation="GetRefreshHistory"
        )
        if not data:
            continue

        for r in data.get("value", []):
            start_time = _parse_ts(r.get("startTime"))
            if start_time:
                aware_start = (
                    start_time.replace(tzinfo=timezone.utc)
                    if start_time.tzinfo is None
                    else start_time
                )
                if aware_start < cutoff:
                    continue   # outside 30-day window

            svc_ex_raw = r.get("serviceExceptionJson") or "{}"
            try:
                svc_ex = json.loads(svc_ex_raw) if svc_ex_raw else {}
            except Exception:
                svc_ex = {}

            rows.append({
                "refresh_id":                str(uuid.uuid4()),
                "model_id":                  model_id,
                "workspace_id":              ws_id,
                "start_time":                start_time,
                "end_time":                  _parse_ts(r.get("endTime")),
                "status":                    r.get("status",      ""),
                "refresh_type":              r.get("refreshType", "") or "",
                "service_exception_code":    svc_ex.get("errorCode",        "") or "",
                "service_exception_message": svc_ex.get("errorDescription", "") or "",
                "extracted_at":              extraction_ts
            })

    print(f"  → {len(rows)} refresh history records")
    return rows


# =============================================================================
# SECTION 11 — EXTRACT: REPORTS
# =============================================================================

def extract_reports(workspace_ids: List[str]) -> List[Dict]:
    print("\n[STEP 8/11] Extracting reports...")
    headers = pbi_headers()
    rows    = []

    for ws_id in workspace_ids:
        for r in paginate(
            f"{PBI_API_BASE}/groups/{ws_id}/reports",
            headers,
            workspace_id=ws_id,
            operation="ListReports"
        ):
            rows.append({
                "report_id":        r.get("id",              ""),
                "workspace_id":     ws_id,
                "report_name":      r.get("name",            ""),
                "report_type":      r.get("reportType",      "") or "",
                "linked_model_id":  r.get("datasetId",       "") or "",
                "web_url":          r.get("webUrl",          "") or "",
                "embed_url":        r.get("embedUrl",        "") or "",
                "created_by_email": r.get("createdBy",       "") or "",
                "created_date":     _parse_ts(r.get("createdDateTime")),
                "modified_date":    _parse_ts(r.get("modifiedDateTime")),
                "extracted_at":     extraction_ts
            })

    print(f"  → {len(rows)} reports")
    return rows


# =============================================================================
# SECTION 12 — EXTRACT: DATAFLOWS
# =============================================================================

def extract_dataflows(workspace_ids: List[str]) -> List[Dict]:
    print("\n[STEP 9/11] Extracting dataflows...")
    headers = pbi_headers()
    rows    = []

    for ws_id in workspace_ids:
        for df in paginate(
            f"{PBI_API_BASE}/groups/{ws_id}/dataflows",
            headers,
            workspace_id=ws_id,
            operation="ListDataflows"
        ):
            df_id = df.get("objectId", "")

            sched = api_get(
                f"{PBI_API_BASE}/groups/{ws_id}/dataflows/{df_id}/refreshSchedule",
                headers,
                workspace_id=ws_id, item_id=df_id,
                item_type="Dataflow",
                item_name=df.get("name", ""),
                operation="GetDataflowRefreshSchedule"
            ) or {}

            rows.append({
                "dataflow_id":            df_id,
                "workspace_id":           ws_id,
                "dataflow_name":          df.get("name",            ""),
                "description":            df.get("description",     "") or "",
                "configured_by":          df.get("configuredBy",    "") or "",
                "is_refresh_scheduled":   bool(sched.get("enabled", False)),
                "refresh_schedule_days":  ",".join(sched.get("days",  [])),
                "refresh_schedule_times": ",".join(sched.get("times", [])),
                "notify_option":          sched.get("notifyOption",                 "") or "",
                "notification_email":     sched.get("notificationGroupMailAddress", "") or "",
                "last_refresh_time":      _parse_ts(df.get("lastRefreshTime")),
                "last_refresh_status":    "",
                "extracted_at":           extraction_ts
            })

    print(f"  → {len(rows)} dataflows")
    return rows


# =============================================================================
# SECTION 13 — EXTRACT: OTHER ASSETS
# =============================================================================

def extract_other_assets(item_rows: List[Dict]) -> List[Dict]:
    print("\n[STEP 10/11] Extracting other assets (Lakehouse, Notebook, Warehouse, etc.)...")
    rows = [
        {
            "asset_id":           r["item_id"],
            "workspace_id":       r["workspace_id"],
            "asset_name":         r["item_name"],
            "asset_type":         r["item_type"],
            "description":        r["description"],
            "created_date":       r["created_date"],
            "last_modified_date": r["last_modified_date"],
            "extracted_at":       extraction_ts
        }
        for r in item_rows
        if r["item_type"] in OTHER_ASSET_TYPES
    ]
    print(f"  → {len(rows)} other assets")
    return rows


# =============================================================================
# SECTION 14 — EXTRACT: POWER QUERY / M EXPRESSIONS
# =============================================================================
#
# Strategy: execute DAX INFO functions via the executeQueries REST endpoint.
#
#   INFO.TABLES()      → maps [TableID] → [Name]  (needed to label partitions)
#   INFO.PARTITIONS()  → one row per partition with [QueryDefinition] (the M / SQL text)
#
# Why DAX INFO functions?
#   - Work for ALL semantic model types: classic .pbix imports, DirectQuery,
#     composite models, and Fabric-native models.
#   - No admin required — Build permission on the dataset is sufficient.
#     Workspace Members/Contributors/Admins all have this by default.
#   - Single API call per dataset instead of needing XMLA or admin Scanner API.
#
# Partition [Type] mapping:
#   1 = Data       (standard import partition — older format, no inline M)
#   2 = M          (Power Query / M expression)
#   3 = Calculated (DAX calculated table)
#   4 = DirectQuery (native SQL / MDX query)
#   7 = Historical  (incremental refresh history partition — older date ranges)
#
# Partition [Mode] mapping:
#   0 = Import         1 = DirectQuery      2 = Default (inherits model setting)
#   3 = Push           4 = Streaming        5 = PushStreaming
# =============================================================================

# Maps INFO.PARTITIONS() [Type] integer to a readable label
_PARTITION_TYPE_MAP = {
    1: "Data",
    2: "M",
    3: "DAX",
    4: "SQL",
    7: "Historical"
}

# Maps INFO.PARTITIONS() [Mode] integer to a readable label
_STORAGE_MODE_MAP = {
    0: "Import",
    1: "DirectQuery",
    2: "Default",
    3: "Push",
    4: "Streaming",
    5: "PushStreaming"
}


def _execute_dax(
    ws_id:    str,
    model_id: str,
    dax:      str,
    model_name: str = ""
) -> Optional[List[Dict]]:
    """
    Executes a DAX query against a semantic model via the executeQueries endpoint.
    Returns the list of row dicts from the first result table, or None on error.

    The Power BI executeQueries API returns column names with bracket notation,
    e.g.  "[TableID]", "[Name]", "[QueryDefinition]".
    This function strips the brackets so callers can use plain key names.
    """
    headers = {**pbi_headers(), "Content-Type": "application/json"}
    body    = {
        "queries":            [{"query": dax}],
        "serializerSettings": {"includeNulls": True}
    }

    try:
        resp = requests.post(
            f"{PBI_API_BASE}/groups/{ws_id}/datasets/{model_id}/executeQueries",
            headers=headers,
            json=body,
            timeout=120
        )
    except Exception as ex:
        _log_access_error(
            workspace_id=ws_id, item_id=model_id,
            item_type="SemanticModel", item_name=model_name,
            operation=f"ExecuteDAX: {dax[:60]}",
            http_status_code=-1, error_body=str(ex), requires_admin=False
        )
        return None

    if resp.status_code == 200:
        try:
            results = resp.json().get("results", [])
            raw_rows = results[0]["tables"][0].get("rows", []) if results else []
            # Strip bracket notation from column names: "[Name]" → "Name"
            return [
                {k.strip("[]"): v for k, v in row.items()}
                for row in raw_rows
            ]
        except (IndexError, KeyError):
            return []   # Query returned no rows — not an error

    elif resp.status_code in (400,):
        # DAX INFO functions not supported on this dataset type (e.g. push-only)
        # Log quietly — not a permissions issue
        _log_access_error(
            workspace_id=ws_id, item_id=model_id,
            item_type="SemanticModel", item_name=model_name,
            operation=f"ExecuteDAX: {dax[:60]}",
            http_status_code=resp.status_code,
            error_body=resp.text, requires_admin=False
        )
        return None

    elif resp.status_code in (401, 403):
        _log_access_error(
            workspace_id=ws_id, item_id=model_id,
            item_type="SemanticModel", item_name=model_name,
            operation=f"ExecuteDAX: {dax[:60]}",
            http_status_code=resp.status_code,
            error_body=resp.text,
            requires_admin=(resp.status_code == 403)
        )
        return None

    else:
        _log_access_error(
            workspace_id=ws_id, item_id=model_id,
            item_type="SemanticModel", item_name=model_name,
            operation=f"ExecuteDAX: {dax[:60]}",
            http_status_code=resp.status_code,
            error_body=resp.text, requires_admin=False
        )
        return None


def extract_power_queries(model_rows: List[Dict]) -> List[Dict]:
    """
    For every semantic model, executes:
        EVALUATE INFO.TABLES()      → table ID ↔ name mapping
        EVALUATE INFO.PARTITIONS()  → partition name + M/SQL query expression

    Returns one row per partition in meta_power_queries.

    Works for:
        ✅  Classic .pbix imports (Import mode)
        ✅  DirectQuery models     (returns the native SQL query)
        ✅  Composite models       (mix of M and SQL per partition)
        ✅  Fabric-native semantic models
        ✅  Incremental refresh models  (each date partition has its own M expression)
        ⚠️  Calculated tables      (DAX expression captured, no M)
        ⚠️  Push / streaming       (no query expression — logged, row still written)

    Permission required: Build permission on the dataset
    (Workspace Members / Contributors / Admins have this by default)
    """
    print("\n[STEP 11/12] Extracting Power Query / M expressions via DAX INFO functions...")
    rows: List[Dict] = []
    skipped = 0

    for m in model_rows:
        ws_id      = m["workspace_id"]
        model_id   = m["model_id"]
        model_name = m.get("model_name", "")

        # ── Step A: get table ID → name map ───────────────────────────────────
        table_rows = _execute_dax(
            ws_id, model_id,
            "EVALUATE INFO.TABLES()",
            model_name
        )
        if table_rows is None:
            skipped += 1
            continue   # Access error already logged

        table_id_to_name: Dict[int, str] = {
            int(t.get("TableID", -1)): t.get("Name", "")
            for t in (table_rows or [])
        }

        # ── Step B: get partitions with query expressions ──────────────────────
        partition_rows = _execute_dax(
            ws_id, model_id,
            "EVALUATE INFO.PARTITIONS()",
            model_name
        )
        if partition_rows is None:
            skipped += 1
            continue

        for p in (partition_rows or []):
            raw_type    = p.get("Type")
            raw_mode    = p.get("Mode")
            table_id    = int(p.get("TableID", -1))
            table_name  = table_id_to_name.get(table_id, f"TableID_{table_id}")
            query_expr  = p.get("QueryDefinition") or ""

            # Skip hidden system / internal partitions that have no query
            # (e.g. Date table auto-generated partitions) unless they have an expression
            part_name = p.get("Name", "")
            if not part_name:
                continue

            partition_type = _PARTITION_TYPE_MAP.get(raw_type, "Unknown")
            storage_mode   = _STORAGE_MODE_MAP.get(raw_mode, "Unknown")

            rows.append({
                "query_id":         str(uuid.uuid4()),
                "model_id":         model_id,
                "model_name":       model_name,
                "workspace_id":     ws_id,
                "table_id":         table_id,
                "table_name":       table_name,
                "partition_name":   part_name,
                "partition_type":   partition_type,    # M / SQL / DAX / Historical / Unknown
                "storage_mode":     storage_mode,      # Import / DirectQuery / etc.
                "query_expression": query_expr,        # The actual M or SQL text
                "extracted_at":     extraction_ts
            })

    total_models   = len(model_rows)
    success_models = total_models - skipped
    print(
        f"  → {len(rows)} partition expressions extracted "
        f"from {success_models}/{total_models} models "
        f"({skipped} skipped — see meta_access_errors)"
    )
    return rows


# =============================================================================
# SECTION 15 — UTILITIES
# =============================================================================

def _parse_ts(value: Any) -> Optional[datetime]:
    """Safely parses an ISO-8601 timestamp string into a Python datetime."""
    if not value:
        return None
    try:
        s = str(value).strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


# =============================================================================
# SECTION 15 — MAIN ORCHESTRATOR
# =============================================================================

def run():
    print("=" * 66)
    print("  FABRIC METADATA EXTRACTOR")
    print(f"  Run started : {extraction_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Scope       : {EXTRACTION_SCOPE.upper()}")
    print("=" * 66)

    # ── Validate scope parameter ──────────────────────────────────────────────
    if EXTRACTION_SCOPE not in ("current", "all"):
        raise ValueError(
            f"Invalid EXTRACTION_SCOPE '{EXTRACTION_SCOPE}'. "
            "Must be 'current' (testing) or 'all' (production)."
        )

    # ── Resolve the workspace this notebook is running in ────────────────────
    context       = notebookutils.runtime.context
    current_ws_id = (
        context.get("currentWorkspaceId")
        or context.get("tridentWorkspaceId")
        or context.get("workspaceId")
    )
    if not current_ws_id:
        raise RuntimeError(
            "Could not determine current workspace ID. "
            "Ensure this notebook is running inside a Fabric workspace."
        )
    print(f"\n[INFO] Current workspace ID : {current_ws_id}")

    # ── Create / locate MetaDataManagement lakehouse ──────────────────────────
    # Always created in the workspace this notebook runs in, regardless of scope.
    lakehouse_id, abfs_root = get_or_create_lakehouse(current_ws_id)

    def save(rows: List[Dict], table_name: str):
        write_table(rows, table_name, lakehouse_id, current_ws_id)

    # ── STEP 1 — Workspaces ───────────────────────────────────────────────────
    if EXTRACTION_SCOPE == "current":
        # Fetch only the current workspace's details via a direct API call
        print("\n[STEP 1/11] Fetching current workspace only (scope=current)...")
        data = api_get(
            f"{FABRIC_API_BASE}/workspaces/{current_ws_id}",
            fabric_headers(),
            workspace_id=current_ws_id,
            operation="GetWorkspace"
        )
        if not data:
            print("\n[ERROR] Could not fetch current workspace details.")
            return
        workspace_rows = [{
            "workspace_id":             data.get("id",          ""),
            "workspace_name":           data.get("displayName", ""),
            "workspace_type":           data.get("type",        ""),
            "state":                    data.get("state",       ""),
            "capacity_id":              data.get("capacityId",  "") or "",
            "description":              data.get("description", "") or "",
            "is_on_dedicated_capacity": bool(data.get("capacityId")),
            "extracted_at":             extraction_ts
        }]
        print(f"  → 1 workspace (current)")

    else:
        # Fetch all workspaces the user has access to
        workspace_rows = extract_workspaces()
        if not workspace_rows:
            print("\n[ERROR] No workspaces returned — verify token scopes and permissions.")
            return

    workspace_ids = [w["workspace_id"] for w in workspace_rows]
    ws_name_map   = {w["workspace_id"]: w["workspace_name"] for w in workspace_rows}

    # ── STEP 2 — Workspace users ──────────────────────────────────────────────
    user_rows = extract_workspace_users(workspace_ids)

    # ── STEP 3 — Workspace items ──────────────────────────────────────────────
    item_rows = extract_workspace_items(workspace_ids)

    # ── STEP 4 — Semantic models ──────────────────────────────────────────────
    model_rows = extract_semantic_models(workspace_ids)

    # ── STEP 5 — Dataflows (needed before datasources) ───────────────────────
    dataflow_rows = extract_dataflows(workspace_ids)

    # ── STEP 6 — Datasources (Semantic Models + Dataflows + Lakehouses) ───────
    datasource_rows = extract_datasources(
        workspace_ids, model_rows, dataflow_rows, item_rows
    )

    # ── STEP 7 — Refresh schedules ────────────────────────────────────────────
    schedule_rows = extract_refresh_schedules(workspace_ids, model_rows)

    # ── STEP 8 — Refresh history ──────────────────────────────────────────────
    history_rows = extract_refresh_history(workspace_ids, model_rows)

    # ── STEP 9 — Reports ──────────────────────────────────────────────────────
    report_rows = extract_reports(workspace_ids)

    # ── STEP 10 — Other assets ────────────────────────────────────────────────
    other_asset_rows = extract_other_assets(item_rows)

    # ── STEP 11 — Power Query / M expressions ────────────────────────────────
    power_query_rows = extract_power_queries(model_rows)

    # ── Enrich access errors with workspace names ─────────────────────────────
    for err in access_errors:
        if not err["workspace_name"] and err["workspace_id"]:
            err["workspace_name"] = ws_name_map.get(err["workspace_id"], "")

    # ── STEP 12 — Write all tables ────────────────────────────────────────────
    print("\n" + "=" * 66)
    print("  [STEP 12/12] WRITING TO LAKEHOUSE: " + LAKEHOUSE_NAME)
    print("=" * 66)

    save(workspace_rows,    "meta_workspaces")
    save(user_rows,         "meta_workspace_users")
    save(item_rows,         "meta_workspace_items")
    save(model_rows,        "meta_semantic_models")
    save(datasource_rows,   "meta_datasources")
    save(schedule_rows,     "meta_refresh_schedules")
    save(history_rows,      "meta_refresh_history")
    save(report_rows,       "meta_reports")
    save(dataflow_rows,     "meta_dataflows")
    save(other_asset_rows,  "meta_other_assets")
    save(power_query_rows,  "meta_power_queries")
    save(access_errors,     "meta_access_errors")   # always last

    # ── Summary ───────────────────────────────────────────────────────────────
    run_end  = datetime.now(timezone.utc)
    duration = int((run_end - extraction_ts).total_seconds())

    print("\n" + "=" * 66)
    print("  EXTRACTION COMPLETE")
    print("=" * 66)
    print(f"  {'Workspaces':<35}: {len(workspace_rows):>6,}")
    print(f"  {'Workspace Users / Role Assignments':<35}: {len(user_rows):>6,}")
    print(f"  {'Workspace Items':<35}: {len(item_rows):>6,}")
    print(f"  {'Semantic Models':<35}: {len(model_rows):>6,}")
    print(f"  {'Datasources (all types)':<35}: {len(datasource_rows):>6,}")
    print(f"    ↳ Semantic Model sources   : "
          f"{sum(1 for r in datasource_rows if r['source_item_type'] == 'SemanticModel'):>5,}")
    print(f"    ↳ Dataflow Gen1 sources    : "
          f"{sum(1 for r in datasource_rows if r['source_item_type'] == 'Dataflow'):>5,}")
    print(f"    ↳ Dataflow Gen2 sources    : "
          f"{sum(1 for r in datasource_rows if r['source_item_type'] == 'DataflowGen2'):>5,}")
    print(f"    ↳ Lakehouse shortcuts      : "
          f"{sum(1 for r in datasource_rows if r['source_item_type'] == 'Lakehouse'):>5,}")
    print(f"  {'Refresh Schedules':<35}: {len(schedule_rows):>6,}")
    print(f"  {'Refresh History Records':<35}: {len(history_rows):>6,}")
    print(f"  {'Reports':<35}: {len(report_rows):>6,}")
    print(f"  {'Dataflows':<35}: {len(dataflow_rows):>6,}")
    print(f"  {'Other Assets':<35}: {len(other_asset_rows):>6,}")
    print(f"  {'Power Query Expressions':<35}: {len(power_query_rows):>6,}")
    print(f"    ↳ M (Power Query) partitions  : "
          f"{sum(1 for r in power_query_rows if r['partition_type'] == 'M'):>5,}")
    print(f"    ↳ SQL (DirectQuery) partitions: "
          f"{sum(1 for r in power_query_rows if r['partition_type'] == 'SQL'):>5,}")
    print(f"    ↳ DAX (Calculated tables)     : "
          f"{sum(1 for r in power_query_rows if r['partition_type'] == 'DAX'):>5,}")
    print(f"  {'Access Errors Logged':<35}: {len(access_errors):>6,}")
    print(f"\n  Lakehouse   : {LAKEHOUSE_NAME} ({lakehouse_id})")
    print(f"  Tables path : {abfs_root}")
    print(f"  Duration    : {duration}s")
    print(f"  Finished at : {run_end.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 66)

    if access_errors:
        print(
            f"\n[NOTE] {len(access_errors)} operations were blocked by permissions.\n"
            "  Review meta_access_errors for details.\n"
            "  Rows with requires_admin=True need Fabric Admin, Dataset Owner, "
            "or Workspace Admin privileges."
        )


# =============================================================================
# ENTRY POINT
# =============================================================================
run()
