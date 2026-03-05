# 🚨 pikud-alerts

Automated dashboard for Israeli Home Front Command (Pikud HaOref) alerts, sourced from the official [@pikud_haoref](https://t.me/pikud_haoref) Telegram channel.

**[Open the Dashboard](https://itzikch.github.io/pikud-alerts/)**

---

## What it does

- Collects alerts from `@pikud_haoref` every 15 minutes via GitHub Actions
- Parses and categorizes alerts across 30 geographic regions in Israel
- Displays an interactive dashboard with charts, rankings, comparisons, and a heat map
- All data is stored in `docs/data.json` and served via GitHub Pages

---

## Setup

### 1. Get Telegram API keys

1. Go to [my.telegram.org](https://my.telegram.org)
2. Log in with your phone number
3. Click **API development tools**
4. Create a new application and note:
   - `api_id` → **TG_API_ID**
   - `api_hash` → **TG_API_HASH**

### 2. Generate a StringSession

Install dependencies and run the one-time session generator:

```bash
pip install telethon
python scripts/gen_session.py
```

The script will prompt for a Telegram verification code and then print a long string — that is your **TG_SESSION** value.

### 3. Add Secrets to GitHub

Go to: `Settings → Secrets and variables → Actions → New repository secret`

| Secret name | Value |
|---|---|
| `TG_API_ID` | The numeric ID from my.telegram.org |
| `TG_API_HASH` | The hash from my.telegram.org |
| `TG_SESSION` | The string printed by gen_session.py |

### 4. Enable GitHub Pages

1. Go to: `Settings → Pages`
2. Under **Source**, select: `Deploy from a branch`
3. Choose branch: `main`, folder: `/docs`
4. Click **Save**

The dashboard will be available at: `https://itzikch.github.io/pikud-alerts/`

---

## Manual trigger

To run the collector on demand:
`Actions → Collect Alerts → Run workflow`

---

## Project structure

```
pikud-alerts/
├── .github/
│   └── workflows/
│       └── collect.yml      # GitHub Actions — runs every 15 minutes
├── scripts/
│   ├── collect.py           # Main collector script
│   └── gen_session.py       # One-time TG_SESSION generator
├── docs/
│   ├── index.html           # The dashboard
│   └── data.json            # Alert data (auto-updated)
└── README.md
```

---

## Dashboard tabs

| Tab | Description |
|---|---|
| **General** | Quick stats, daily bar chart, recent alerts table |
| **By Region** | Bar chart of all regions + daily trend for a selected region |
| **Ranking** | Sorted list of most-hit regions with progress bars |
| **Compare** | Line chart comparing the top 10 regions over time |
| **Heat Map** | Grid of regions × days (last 30 days) |

---

## Covered regions (30)

Shomron, HaShfela, Yehuda, Lachish, Sharon, Yarkon, Upper Galilee, Shfela Yehuda,
HaAmakim, Bika, Line of Confrontation, Dan, South Golan, Lower Galilee, Central Galilee,
Dead Sea, Carmel, South Negev, Wadi Ara, Beit Shean Valley, West Negev, The Bay,
Jerusalem, West Lachish, Central Negev, Menashe, Gaza Envelope, North Golan, Arava, Eilat

---

## Notes

- `FloodWaitError` from Telegram is handled gracefully — the script exits silently and retries on the next run
- `data.json` is cumulative — it is never reset between runs
- `[skip ci]` in the auto-commit message prevents an Actions loop
