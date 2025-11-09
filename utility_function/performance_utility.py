
from utility_function.initilize_dbconnection import supabase
import streamlit as st
import pandas as pd

# ========================
# Name → ID Maps
# ========================

def _safe_dict(lst, key_name, id_name):
    try:
        return {row.get(key_name): row.get(id_name) for row in lst if row.get(key_name) and row.get(id_name)}
    except Exception:
        return {}

def get_club_competitions():
    try:
        res = supabase.table("club_competition").select("club_competition_id,name").execute()
        return _safe_dict(res.data or [], "name", "club_competition_id")
    except Exception as e:
        st.warning(f"Could not load club competitions: {e}")
        return {}

def get_yearly_championships():
    try:
        res = supabase.table("yearly_club_championship").select("yearly_club_championship_id,name").execute()
        return _safe_dict(res.data or [], "name", "yearly_club_championship_id")
    except Exception as e:
        st.warning(f"Could not load yearly club championships: {e}")
        return {}

def get_rounds():
    try:
        res = supabase.table("round").select("round_id,name").execute()
        return _safe_dict(res.data or [], "name", "round_id")
    except Exception as e:
        st.warning(f"Could not load rounds: {e}")
        return {}

def get_categories():
    try:
        res = supabase.table("category").select("category_id").execute()
        return _safe_dict(res.data or [], "category_id", "category_id")
    except Exception as e:
        st.warning(f"Could not load categories: {e}")
        return {}

def get_archers():
    """Return {fullname: account_id} for accounts with role 'archer'."""
    try:
        res = supabase.table("account").select("account_id,fullname,role").eq("role","archer").execute()
        data = res.data or []
        return {row["fullname"]: row["account_id"] for row in data if row.get("fullname") and row.get("account_id")}
    except Exception as e:
        st.warning(f"Could not load archers: {e}")
        return {}

# ========================
# Helper utilities
# ========================

def _compute_sum_score(row):
    keys = ["score_1st_arrow","score_2nd_arrow","score_3rd_arrow","score_4th_arrow","score_5th_arrow","score_6th_arrow"]
    if all(k in row for k in keys):
        return sum((row.get(k) or 0) for k in keys)
    return row.get("sum_score", 0)

def _participant_label(rec):
    name = None
    if isinstance(rec.get("archer"), dict):
        archer = rec.get("archer")
        if isinstance(archer.get("account"), dict):
            name = archer["account"].get("fullname")
    name = name or rec.get("fullname") or "Unknown"
    pid = rec.get("participating_id")
    return f"{name} ({pid})" if pid else name

# ========================
# Fetchers
# ========================

def _fetch_participating_base(club_competition_id=None, round_id=None, archer_account_id=None):
    """Base fetcher: participating + event_context + archer→account join."""
    try:
        query = supabase.table("participating").select(
            "participating_id, archer_id, sum_score, "
            "score_1st_arrow,score_2nd_arrow,score_3rd_arrow,score_4th_arrow,score_5th_arrow,score_6th_arrow, "
            "event_context!inner(event_context_id,club_competition_id,round_id,range_id,end_order), "
            "archer!inner(account!inner(fullname))"
        ).eq("type","competition")

        if club_competition_id:
            query = query.eq("event_context.club_competition_id", club_competition_id)
        if round_id:
            query = query.eq("event_context.round_id", round_id)
        if archer_account_id:
            query = query.eq("participating_id", archer_account_id)

        res = query.execute()
        return res.data or []
    except Exception as e:
        st.warning(f"Error fetching participating data: {e}")
        return []

# ---------------------------
# View Sum Score - helpers
# ---------------------------
def fetch_scores_per_end(club_competition_id=None, round_id=None, archer_account_id=None):
    rows = _fetch_participating_base(club_competition_id, round_id, archer_account_id)
    if not rows: return pd.DataFrame()
    out = []
    for r in rows:
        label = _participant_label(r)
        sum_score = _compute_sum_score(r)
        end = (r.get("event_context") or {}).get("end_order")
        out.append({"participant": label, "end_order": end, "sum_score": sum_score})
    df = pd.DataFrame(out)
    return df.groupby(["participant","end_order"], as_index=False)["sum_score"].sum()

def fetch_scores_per_range(club_competition_id=None, round_id=None, archer_account_id=None):
    rows = _fetch_participating_base(club_competition_id, round_id, archer_account_id)
    if not rows: return pd.DataFrame()
    out = []
    for r in rows:
        label = _participant_label(r)
        sum_score = _compute_sum_score(r)
        rng = (r.get("event_context") or {}).get("range_id")
        out.append({"participant": label, "range_id": rng, "sum_score": sum_score})
    df = pd.DataFrame(out)
    return df.groupby(["participant","range_id"], as_index=False)["sum_score"].sum()

def fetch_scores_per_round(club_competition_id=None, round_id=None, archer_account_id=None):
    rows = _fetch_participating_base(club_competition_id, round_id, archer_account_id)
    if not rows: return pd.DataFrame()
    out = []
    for r in rows:
        label = _participant_label(r)
        sum_score = _compute_sum_score(r)
        rnd = (r.get("event_context") or {}).get("round_id")
        out.append({"participant": label, "round_id": rnd, "sum_score": sum_score})
    df = pd.DataFrame(out)
    return df.groupby(["participant","round_id"], as_index=False)["sum_score"].sum().sort_values("sum_score", ascending=False)

# ---------------------------
# Yearly normalized average
# ---------------------------
def _max_score_for_round(round_id):
    try:
        q = supabase.table("event_context").select("round_id, range_id, end_order").eq("round_id", round_id).execute()
        rows = q.data or []
        if not rows: 
            return None
        ends = {(r.get("range_id"), r.get("end_order")) for r in rows if r.get("end_order") is not None}
        total_arrows = len(ends) * 6
        return total_arrows * 10 if total_arrows else None
    except Exception as e:
        st.info(f"Could not derive max score for round {round_id}: {e}")
        return None

def fetch_yearly_normalized_average(yc_id=None, round_id=None, archer_account_id=None):
    if not yc_id or not round_id:
        st.info("Please select both a Yearly Club Championship and a Round.")
        return pd.DataFrame()
    try:
        query = supabase.table("event_context").select("club_competition_id").eq("yearly_club_championship_id", yc_id)
        if round_id:
            query = query.eq("round_id", round_id)
        comp_res = query.execute()
        comp_ids = sorted({r.get("club_competition_id") for r in (comp_res.data or []) if r.get("club_competition_id")})
        
        if not comp_ids:
            st.info("No competitions found for this yearly championship (check event context links).")            
            return pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not load competitions for yearly championship: {e}")
        return pd.DataFrame()

    try:
        query = supabase.table("participating").select(
            "participating_id, archer_id, sum_score, "
            "score_1st_arrow,score_2nd_arrow,score_3rd_arrow,score_4th_arrow,score_5th_arrow,score_6th_arrow, "
            "event_context!inner(club_competition_id,round_id), "
            "archer!inner(account!inner(fullname))"
        ).eq("type","competition").eq("event_context.round_id", round_id).in_("event_context.club_competition_id", comp_ids)
        if archer_account_id:
            query = query.eq("participating_id", archer_account_id)
        res = query.execute()
        rows = res.data or []
    except Exception as e:
        st.warning(f"Could not load participating rows: {e}")
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    max_score = _max_score_for_round(round_id)
    if not max_score:
        st.info("Could not determine max score for the selected round — normalized average will fallback to raw average. # placeholder")
        max_score = None

    out = []
    for r in rows:
        label = _participant_label(r)
        s = _compute_sum_score(r)
        norm = (s / max_score) if max_score else None
        out.append({"participant": label, "sum_score": s, "normalized": norm})

    df = pd.DataFrame(out)
    if "normalized" in df.columns and df["normalized"].notna().any():
        agg = df.groupby("participant", as_index=False).agg(normalized_avg=("normalized","mean"))
        return agg.sort_values("normalized_avg", ascending=False)
    else:
        agg = df.groupby("participant", as_index=False).agg(raw_avg=("sum_score","mean"))
        return agg.sort_values("raw_avg", ascending=False)

# ---------------------------
# Rankings
# ---------------------------
def fetch_ranking_in_round(club_competition_id=None, round_id=None):
    rows = _fetch_participating_base(club_competition_id, round_id, None)
    if not rows: return pd.DataFrame()
    out = [{"participant": _participant_label(r), "sum_score": _compute_sum_score(r)} for r in rows]
    df = pd.DataFrame(out)
    return df.groupby("participant", as_index=False)["sum_score"].sum().sort_values("sum_score", ascending=False)

def fetch_ranking_yearly_same_round(yc_id=None, round_id=None):
    df = fetch_yearly_normalized_average(yc_id, round_id, None)
    return df

# ---------------------------
# Category percentile
# ---------------------------
def fetch_category_percentile_distribution(archer_account_id=None, category_id=None):
    if not category_id:
        st.info("Please select a category.")
        return pd.DataFrame(), None
    try:
        q = supabase.table("category_rating_percentile").select("archer_id, category_id, percentile").eq("category_id", category_id)
        res = q.execute()
        rows = res.data or []
    except Exception as e:
        st.warning(f"Could not load category rating distribution: {e}")
        return pd.DataFrame(), None
    if not rows:
        return pd.DataFrame(), None

    df = pd.DataFrame(rows).rename(columns={"archer_id":"archer_account_id","percentile":"c_score"})
    df = df[["archer_account_id","c_score"]].dropna()

    my_percentile = None
    if archer_account_id is not None and archer_account_id in set(df["archer_account_id"]):
        d_sorted = df.sort_values("c_score").reset_index(drop=True)
        idx_list = d_sorted.index[d_sorted["archer_account_id"] == archer_account_id].tolist()
        if idx_list:
            idx0 = idx_list[0]
            m = len(d_sorted)
            my_percentile = ((idx0 + 1) / m) * 100.0

    return df, my_percentile
