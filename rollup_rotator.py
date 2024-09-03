#!/usr/bin/env python3
"""
Rollup Rotator — resilient access to Rollup RPC with health checks, scoring and failover.

Purpose:
- Minimize failed calls and latency when reading/sending TXs to rollup chains.
- Keep a sticky 'best' endpoint while periodically re-evaluating candidates.

Features:
- Keeps an RPC pool in JSON (default: endpoints.json).
- Active checks: latency, head drift vs reference, simple error handling.
- Scores endpoints and selects the best (sticky session saved to current.json).
- CLI: add/remove/list/test/best/pick/use.
- Standard library only (no external deps).

endpoints.json format:
{
  "chain_id": 42161,
  "endpoints": [
    {"url": "https://arb1.private.example/rpc", "tag": "priv1"},
    {"url": "https://arb1.publicnode.com",     "tag": "pub-a"}
  ]
}
"""

import json, os, time, random, sys, urllib.request, urllib.error
from typing import List, Dict, Any, Tuple

ENDPOINTS_FILE = "endpoints.json"
CURRENT_FILE   = "current.json"
TIMEOUT_SEC    = 6           # HTTP timeout per request
HEAD_SKEW_OK   = 3           # acceptable block lag (in blocks)
LAT_BUDGET_MS  = 1200        # "good enough" latency budget for scoring (ms)

def _rpc_post(url: str, method: str, params: list) -> Tuple[bool, Any, float]:
    """Perform a JSON-RPC POST. Returns (ok, result_or_error, latency_ms)."""
    payload = {"jsonrpc":"2.0","id":1,"method":method,"params":params}
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={
        "Content-Type":"application/json",
        "Accept":"application/json",
        "User-Agent":"rollup-rotator/1.0"
    })
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
            raw = resp.read()
            latency = (time.time() - t0) * 1000.0
            obj = json.loads(raw.decode() or "{}")
            if "error" in obj:
                return False, obj["error"], latency
            return True, obj.get("result"), latency
    except urllib.error.URLError as e:
        latency = (time.time() - t0) * 1000.0
        return False, {"message": str(e)}, latency
    except Exception as e:
        latency = (time.time() - t0) * 1000.0
        return False, {"message": f"unexpected: {e}"}, latency

def _hex_to_int(x: str) -> int:
    try:
        return int(x, 16)
    except Exception:
        return -1

def _load(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)

def _save(obj: dict, path: str):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)

def _best_height(candidates: List[str]) -> int:
    """Probe several endpoints and return the max observed block height."""
    best = -1
    sample = candidates[:]
    random.shuffle(sample)
    sample = sample[: min(len(sample), 3)]
    for u in sample:
        ok, res, _ = _rpc_post(u, "eth_blockNumber", [])
        if ok:
            best = max(best, _hex_to_int(res))
    return best

def _score_endpoint(url: str, head_ref: int) -> dict:
    """Check a single endpoint and compute a score in [0..1]."""
    ok_b, res_b, lat = _rpc_post(url, "eth_blockNumber", [])
    head = -1
    if ok_b:
        head = _hex_to_int(res_b)
        drift = 0 if head_ref < 0 else max(0, head_ref - head)
        # Normalize latency and drift into penalties
        lat_p   = min(1.0, lat / LAT_BUDGET_MS)
        drift_p = 0.0 if drift <= HEAD_SKEW_OK else min(1.0, (drift - HEAD_SKEW_OK) / 10.0)
        score = max(0.0, 1.0 - 0.6*lat_p - 0.4*drift_p)
        return {"url": url, "ok": True, "lat_ms": round(lat,1), "head": head, "drift": drift, "score": round(score,3)}
    else:
        return {"url": url, "ok": False, "lat_ms": round(lat,1), "head": head, "drift": None, "score": 0.0, "error": res_b}

def cmd_list(args):
    cfg = _load(args.file)
    eps = cfg.get("endpoints", [])
    if not eps:
        print("No endpoints. Use: add --url <rpc> [--tag <tag>]"); return
    for i, e in enumerate(eps):
        print(f"{i:02d}. {e.get('url')}  tag={e.get('tag','-')}")

def cmd_add(args):
    cfg = _load(args.file)
    eps = cfg.setdefault("endpoints", [])
    eps.append({"url": args.url, "tag": args.tag or ""})
    _save(cfg, args.file)
    print("Added:", args.url, "tag=", args.tag or "-")

def cmd_remove(args):
    cfg = _load(args.file)
    eps = cfg.get("endpoints", [])
    before = len(eps)
    eps = [e for e in eps if e.get("url") != args.url]
    cfg["endpoints"] = eps
    _save(cfg, args.file)
    print("Removed" if len(eps) < before else "Nothing to remove:", args.url)

def cmd_test(args):
    cfg = _load(args.file)
    eps = [e.get("url") for e in cfg.get("endpoints", [])]
    if not eps:
        print("No endpoints to test"); return
    head_ref = _best_height(eps)
    rows = [_score_endpoint(u, head_ref) for u in eps]
    rows.sort(key=lambda r: r["score"], reverse=True)
    for r in rows:
        line = f"{r['score']:>4}  {r['lat_ms']:>6}ms  head={r['head']:<10}  url={r['url']}"
        if not r["ok"]:
            line += f"  ERR={r.get('error',{})}"
        print(line)

def cmd_best(args):
    cfg = _load(args.file)
    eps = [e.get("url") for e in cfg.get("endpoints", [])]
    if not eps:
        print("No endpoints."); return
    head_ref = _best_height(eps)
    best = max((_score_endpoint(u, head_ref) for u in eps), key=lambda r: r["score"])
    print(json.dumps(best, indent=2))

def cmd_pick(args):
    """Pick the best endpoint and save it to current.json (sticky session)."""
    cfg = _load(args.file)
    eps = [e.get("url") for e in cfg.get("endpoints", [])]
    if not eps:
        print("No endpoints."); return
    head_ref = _best_height(eps)
    scored = [_score_endpoint(u, head_ref) for u in eps]
    scored.sort(key=lambda r: r["score"], reverse=True)
    best = scored[0]
    cur = {"picked_at": int(time.time()), "url": best["url"], "score": best["score"], "lat_ms": best["lat_ms"], "head": best["head"]}
    _save(cur, args.current)
    print("Picked:", best["url"], "score=", best["score"])

def cmd_use(args):
    """Print the current (or freshly picked) endpoint URL — useful in shell scripts."""
    cur = _load(args.current)
    if cur.get("url"):
        print(cur["url"]); return
    # Fallback: pick best if no sticky selection exists
    class P: pass
    P.file = args.file; P.current = args.current
    return cmd_pick(P)

def build_argparser():
    import argparse
    ap = argparse.ArgumentParser(description="Rollup RPC Rotator with health checks and failover")
    ap.add_argument("--file", default=ENDPOINTS_FILE, help="endpoints json (default endpoints.json)")
    ap.add_argument("--current", default=CURRENT_FILE, help="sticky session file (default current.json)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("list", help="list endpoints");      p.set_defaults(func=cmd_list)
    p = sub.add_parser("add", help="add endpoint");         p.add_argument("--url", required=True); p.add_argument("--tag"); p.set_defaults(func=cmd_add)
    p = sub.add_parser("remove", help="remove endpoint");   p.add_argument("--url", required=True); p.set_defaults(func=cmd_remove)
    p = sub.add_parser("test", help="test & score endpoints"); p.set_defaults(func=cmd_test)
    p = sub.add_parser("best", help="print best endpoint json"); p.set_defaults(func=cmd_best)
    p = sub.add_parser("pick", help="pick best -> current.json"); p.set_defaults(func=cmd_pick)
    p = sub.add_parser("use",  help="print current (or pick best) url"); p.set_defaults(func=cmd_use)
    return ap

def main():
    ap = build_argparser()
    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
