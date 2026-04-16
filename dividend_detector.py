import gspread
import yfinance as yf
from google.oauth2.service_account import Credentials
import json, os, time
from datetime import date, datetime, timedelta

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# ── Colonnes dans "Portfolio" (0-indexé) ────────────────────────────
COL_YAHOO    = 11   # L  → ticker Yahoo
COL_CURRENCY = 13   # N  → devise
COL_NAME     = 15   # P  → nom entreprise
COL_QTY      = 17   # R  → quantité détenue

PORTFOLIO_SHEET = 'Portfolio'
PENDING_SHEET   = 'Dividend pending'
START_ROW       = 5   # index 0 = row 1, donc row 6 = index 5


# ── Auth Google Sheets ───────────────────────────────────────────────
def get_gspread_client():
    creds_info = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet_ids():
    raw = os.environ.get('SPREADSHEET_IDS', '')
    return [s.strip() for s in raw.split(',') if s.strip()]


# ── Lecture du portfolio ─────────────────────────────────────────────
def read_portfolio(sheet):
    all_rows = sheet.get_all_values()
    holdings = []

    for row in all_rows[START_ROW:]:
        if len(row) <= COL_QTY:
            row += [''] * (COL_QTY + 1 - len(row))

        yahoo_ticker = row[COL_YAHOO].strip()
        name         = row[COL_NAME].strip()
        qty_raw      = row[COL_QTY].strip().replace(',', '.')
        currency     = row[COL_CURRENCY].strip()

        if not yahoo_ticker or not qty_raw:
            continue
        try:
            qty = float(qty_raw)
        except ValueError:
            continue

        holdings.append({
            'ticker':   yahoo_ticker,
            'name':     name,
            'quantity': qty,
            'currency': currency
        })

    return holdings


# ── Anti-doublon ─────────────────────────────────────────────────────
def get_existing_keys(pending_sheet):
    existing = set()
    try:
        rows = pending_sheet.get_all_values()
        for row in rows:
            if len(row) >= 3 and row[1] and row[2]:
                existing.add(f"{row[1].strip()}|{row[2].strip()}")
    except Exception:
        pass
    return existing


# ── Récupération du dividende via yfinance ───────────────────────────
def fetch_dividend_info(yahoo_ticker, today):
    try:
        t = yf.Ticker(yahoo_ticker)
        info = t.info

        ex_ts = info.get('exDividendDate')
        if not ex_ts:
            return None

        ex_date = date.fromtimestamp(int(ex_ts))
        if ex_date != today:
            return None

        hist = t.dividends
        if not hist.empty:
            amount = float(hist.iloc[-1])
        else:
            amount = info.get('lastDividendValue') or info.get('dividendRate', 0)
            if not amount:
                print(f"  [{yahoo_ticker}] Montant introuvable, ignoré.")
                return None

        pay_date = _get_pay_date(t, ex_date)

        return {'ex_date': ex_date, 'pay_date': pay_date, 'amount': float(amount)}

    except Exception as e:
        print(f"  [{yahoo_ticker}] Erreur yfinance : {e}")
        return None


def _get_pay_date(ticker_obj, ex_date):
    try:
        cal = ticker_obj.calendar
        if isinstance(cal, dict):
            d = cal.get('Dividend Date')
        elif hasattr(cal, 'get'):
            d = cal.get('Dividend Date')
        else:
            d = None

        if d is not None:
            if isinstance(d, datetime):
                return d.date()
            if isinstance(d, date):
                return d
    except Exception:
        pass

    return ex_date + timedelta(weeks=3)


# ── Traitement d'un spreadsheet ──────────────────────────────────────
def process_spreadsheet(gc, spreadsheet_id, today):
    print(f"\n── Spreadsheet : {spreadsheet_id}")

    try:
        ss = gc.open_by_key(spreadsheet_id)
    except Exception as e:
        print(f"  Impossible d'ouvrir le fichier : {e}")
        return

    try:
        portfolio_sh = ss.worksheet(PORTFOLIO_SHEET)
    except Exception:
        print(f"  Onglet '{PORTFOLIO_SHEET}' introuvable.")
        return

    try:
        pending_sh = ss.worksheet(PENDING_SHEET)
    except Exception:
        print(f"  Onglet '{PENDING_SHEET}' introuvable.")
        return

    holdings = read_portfolio(portfolio_sh)
    print(f"  {len(holdings)} ligne(s) dans le portfolio.")

    existing_keys = get_existing_keys(pending_sh)
    rows_to_add   = []

    for h in holdings:
        ticker = h['ticker']
        print(f"  Vérifie {ticker}...", end=' ')

        result = fetch_dividend_info(ticker, today)

        if result is None:
            print("pas de dividende aujourd'hui.")
            time.sleep(0.3)
            continue

        pay_date_str = result['pay_date'].strftime('%d/%m/%Y')
        total_amount = round(result['amount'] * h['quantity'], 4)
        dedup_key    = f"{h['name']}|{pay_date_str}"

        if dedup_key in existing_keys:
            print(f"doublon ignoré ({pay_date_str}).")
            continue

        rows_to_add.append([
            '',
            h['name'],
            pay_date_str,
            total_amount,
            h['currency']
        ])

        existing_keys.add(dedup_key)
        print(f"✓ ex={result['ex_date']} pay={pay_date_str} montant={total_amount} {h['currency']}")
        time.sleep(0.3)

    if not rows_to_add:
        print("  Aucun nouveau dividende à inscrire.")
        return

    next_row = max(len(pending_sh.get_all_values()) + 1, 1)
    end_row  = next_row + len(rows_to_add) - 1

    pending_sh.update(
        range_name=f'A{next_row}:E{end_row}',
        values=rows_to_add,
        value_input_option='USER_ENTERED'
    )
    print(f"  → {len(rows_to_add)} dividende(s) inscrit(s) (lignes {next_row}–{end_row}).")


# ── Main ─────────────────────────────────────────────────────────────
def main():
    today = date.today()
    print(f"=== Dividend Detector — {today.strftime('%d/%m/%Y')} ===")

    gc  = get_gspread_client()
    ids = get_spreadsheet_ids()

    if not ids:
        print("Aucun SPREADSHEET_ID configuré dans les secrets.")
        return

    for sid in ids:
        process_spreadsheet(gc, sid, today)

    print("\n=== Terminé ===")


if __name__ == '__main__':
    main()
