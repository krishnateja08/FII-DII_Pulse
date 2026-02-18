name: FII/DII Intelligence Dashboard

on:
  schedule:
    - cron: "30 13 * * 1-5"   # 07:00 PM IST Mon-Fri
    - cron: "0 4 * * 2-6"     # 09:30 AM IST Tue-Sat
  workflow_dispatch:
    inputs:
      reason:
        description: "Reason for manual run"
        required: false
        default: "Manual trigger"

permissions:
  contents: write
  pages: write
  id-token: write

jobs:
  # ── JOB 1: Generate report and push docs/ to gh-pages branch ─────────────
  generate:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - name: Checkout main branch
        uses: actions/checkout@v4
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
          fetch-depth: 0   # need full history for gh-pages push

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install \
            requests pandas numpy yfinance \
            beautifulsoup4 lxml pytz \
            python-dotenv curl_cffi

      - name: Generate FII/DII Dashboard
        env:
          GMAIL_USER:      ${{ secrets.GMAIL_USER }}
          GMAIL_PASS:      ${{ secrets.GMAIL_PASS }}
          RECIPIENT_EMAIL: ${{ secrets.RECIPIENT_EMAIL }}
        run: python "FII&DII_stock_act.py"

      - name: Save log artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: dashboard-log-${{ github.run_number }}
          path: dashboard.log
          retention-days: 7

      # Push docs/ folder contents → gh-pages branch (root)
      - name: Deploy docs/ to gh-pages branch
        uses: peaceiris/actions-gh-pages@v4
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./docs          # contents of docs/ become root of gh-pages
          publish_branch: gh-pages
          commit_message: "📊 Auto-update FII/DII report ${{ github.run_number }}"
          keep_files: false            # replace with fresh report each time
