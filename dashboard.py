import duckdb
from http.server import HTTPServer, BaseHTTPRequestHandler

DB_PATH = "data/crypto.duckdb"

def safe(val, default=0):
    return val if val is not None else default

def fmt_price(val):
    if val is None: return "N/A"
    if val >= 1000: return f"${val:,.2f}"
    if val >= 1: return f"${val:.4f}"
    return f"${val:.6f}"

def fmt_mcap(val):
    if val is None: return "N/A"
    if val >= 1_000_000_000: return f"${val/1_000_000_000:.2f}B"
    if val >= 1_000_000: return f"${val/1_000_000:.2f}M"
    return f"${val:,.0f}"

def pct_color(val):
    if val is None: return "white"
    return "#00e676" if val > 0 else "#ff5252"

def get_data():
    con = duckdb.connect(DB_PATH)
    rankings = con.execute("""
        SELECT market_cap_rank, symbol, name,
               current_price_usd, pct_change_24h,
               pct_change_7d, market_cap_usd
        FROM gold_market_cap_rankings LIMIT 20
    """).fetchall()

    movers = con.execute("""
        SELECT symbol, name, current_price_usd,
               pct_change_24h, mover_type
        FROM gold_top_movers LIMIT 10
    """).fetchall()

    volatility = con.execute("""
        SELECT symbol, name, current_price_usd,
               volatility_pct, volatility_band
        FROM gold_volatility_scores LIMIT 10
    """).fetchall()

    bronze_count = con.execute("SELECT COUNT(*) FROM bronze_crypto_prices").fetchone()[0]
    silver_count = con.execute("SELECT COUNT(*) FROM silver_crypto_prices").fetchone()[0]
    gold_count   = con.execute("SELECT COUNT(*) FROM gold_market_cap_rankings").fetchone()[0]
    con.close()
    return rankings, movers, volatility, bronze_count, silver_count, gold_count

def build_html():
    rankings, movers, volatility, bronze_count, silver_count, gold_count = get_data()

    ranking_rows = ""
    for r in rankings:
        rank, symbol, name, price, chg24, chg7, mcap = r
        chg24 = safe(chg24)
        chg7  = safe(chg7)
        arrow = "▲" if chg24 > 0 else "▼"
        ranking_rows += f"""<tr>
            <td>#{rank}</td>
            <td><strong>{symbol}</strong></td>
            <td>{name}</td>
            <td>{fmt_price(price)}</td>
            <td style="color:{pct_color(chg24)}">{arrow} {chg24:.2f}%</td>
            <td style="color:{pct_color(chg7)}">{chg7:.2f}%</td>
            <td>{fmt_mcap(mcap)}</td>
        </tr>"""

    mover_rows = ""
    for m in movers:
        symbol, name, price, chg24, mtype = m
        chg24 = safe(chg24)
        emoji = "🚀" if mtype == "gainer" else "📉"
        mover_rows += f"""<tr>
            <td>{emoji} <strong>{symbol}</strong></td>
            <td>{name}</td>
            <td>{fmt_price(price)}</td>
            <td style="color:{pct_color(chg24)}">{chg24:.2f}%</td>
            <td><span class="badge {mtype}">{mtype.upper()}</span></td>
        </tr>"""

    vol_rows = ""
    for v in volatility:
        symbol, name, price, vpct, band = v
        vpct = safe(vpct)
        band = band or "LOW"
        vol_rows += f"""<tr>
            <td><strong>{symbol}</strong></td>
            <td>{name}</td>
            <td>{fmt_price(price)}</td>
            <td>{vpct:.2f}%</td>
            <td><span class="badge {band.lower()}">{band}</span></td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Crypto Pipeline Dashboard</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:#0d1117; color:#e6edf3; font-family:-apple-system,sans-serif; }}
  header {{ background:linear-gradient(135deg,#1e3a5f,#2e75b6); padding:30px 40px; }}
  header h1 {{ font-size:26px; font-weight:700; }}
  header p {{ color:#a0b4c8; margin-top:6px; font-size:14px; }}
  .stats {{ display:flex; gap:20px; padding:30px 40px 0; }}
  .stat-card {{ background:#161b22; border:1px solid #30363d; border-radius:10px; padding:20px 28px; flex:1; }}
  .stat-card .label {{ color:#8b949e; font-size:12px; margin-bottom:6px; text-transform:uppercase; }}
  .stat-card .value {{ font-size:30px; font-weight:700; color:#2e75b6; }}
  .section {{ padding:30px 40px; }}
  .section h2 {{ font-size:17px; font-weight:600; margin-bottom:16px; padding-bottom:8px; border-bottom:2px solid #2e75b6; }}
  table {{ width:100%; border-collapse:collapse; background:#161b22; border-radius:10px; overflow:hidden; }}
  th {{ background:#1e3a5f; padding:12px 16px; text-align:left; font-size:12px; color:#8b949e; text-transform:uppercase; }}
  td {{ padding:12px 16px; border-bottom:1px solid #21262d; font-size:14px; }}
  tr:last-child td {{ border-bottom:none; }}
  tr:hover td {{ background:#1c2128; }}
  .grid {{ display:grid; grid-template-columns:1fr 1fr; }}
  .badge {{ padding:3px 10px; border-radius:20px; font-size:11px; font-weight:600; }}
  .gainer {{ background:#0d4a1e; color:#00e676; }}
  .loser  {{ background:#4a0d0d; color:#ff5252; }}
  .high   {{ background:#4a1a0d; color:#ff6d00; }}
  .medium {{ background:#4a3a0d; color:#ffd740; }}
  .low    {{ background:#0d4a1e; color:#00e676; }}
  footer {{ text-align:center; padding:30px; color:#8b949e; font-size:12px; border-top:1px solid #21262d; }}
</style>
</head>
<body>

<header>
  <h1>🚀 Crypto Market Analytics Pipeline</h1>
  <p>Real-time data · CoinGecko API · Python · DuckDB · dbt · Airflow</p>
</header>

<div class="stats">
  <div class="stat-card">
    <div class="label">Bronze Records</div>
    <div class="value">{bronze_count:,}</div>
  </div>
  <div class="stat-card">
    <div class="label">Silver Records</div>
    <div class="value">{silver_count:,}</div>
  </div>
  <div class="stat-card">
    <div class="label">Coins Tracked</div>
    <div class="value">{gold_count}</div>
  </div>
  <div class="stat-card">
    <div class="label">Pipeline Status</div>
    <div class="value" style="color:#00e676">LIVE ✓</div>
  </div>
</div>

<div class="section">
  <h2>📊 Market Cap Rankings</h2>
  <table>
    <thead><tr>
      <th>Rank</th><th>Symbol</th><th>Name</th>
      <th>Price</th><th>24h</th><th>7d</th><th>Market Cap</th>
    </tr></thead>
    <tbody>{ranking_rows}</tbody>
  </table>
</div>

<div class="grid">
  <div class="section">
    <h2>🔥 Top Movers (24h)</h2>
    <table>
      <thead><tr>
        <th>Symbol</th><th>Name</th><th>Price</th><th>Change</th><th>Type</th>
      </tr></thead>
      <tbody>{mover_rows}</tbody>
    </table>
  </div>
  <div class="section">
    <h2>⚡ Volatility Scores</h2>
    <table>
      <thead><tr>
        <th>Symbol</th><th>Name</th><th>Price</th><th>Volatility</th><th>Band</th>
      </tr></thead>
      <tbody>{vol_rows}</tbody>
    </table>
  </div>
</div>

<footer>Crypto Market Analytics Pipeline · github.com/Kumaresh64/crypto-pipeline · Kumaresh 2026</footer>
</body>
</html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(build_html().encode())
    def log_message(self, format, *args):
        pass

if __name__ == "__main__":
    server = HTTPServer(('localhost', 5000), Handler)
    print("✅ Dashboard running at http://localhost:5000")
    print("   Press Ctrl+C to stop")
    server.serve_forever()
