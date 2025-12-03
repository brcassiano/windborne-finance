import re


def _read_default_companies():
    cfg_path = 'etl/config.py'
    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            text = f.read()
    except FileNotFoundError:
        return ['TEL', 'ST', 'DD']

    m = re.search(r"TARGET_COMPANIES:\s*str\s*=\s*\"([A-Z0-9, ]+)\"", text)
    if m:
        return [c.strip() for c in m.group(1).split(',') if c.strip()]
    return ['TEL', 'ST', 'DD']


def main():
    companies = _read_default_companies()
    total_calls = 0
    print(f"Companies configured: {companies}")
    for symbol in companies:
        # Simulate fetch_all_statements returning 3 statement types
        statements = {'INCOME': {}, 'BALANCE': {}, 'CASHFLOW': {}}
        print(f"Processing {symbol}: simulated statements returned = {len(statements)}")
        total_calls += len(statements)

    print("\nSummary:")
    print(f"  Companies processed: {len(companies)}")
    print(f"  Total API calls (simulated): {total_calls}")

if __name__ == '__main__':
    main()
