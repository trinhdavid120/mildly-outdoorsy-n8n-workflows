# Sticker Recreator

Find **single** Etsy / Amazon sticker listings that are clearly *selling*, throw
out anything copyrighted or trademarked, and turn each survivor into an **85–90%
recreation brief** — same joke, our own words, our own art, plus a ready-to-run
image-generation prompt and a first-draft mockup.

It's built to slot into the existing Mildly Outdoorsy stack (fal.ai for images,
Supabase "Design For Review" queue, n8n schedulers). The image generator is a
plug so you can **wire the token later** — until then a `dryrun` provider draws a
placeholder sticker so the output folder is populated from day one.

---

## The idea in one picture

```
 scrape (Etsy/Amazon)          analyze                 brief                generate            write
┌─────────────────────┐   ┌──────────────────┐   ┌──────────────────┐  ┌───────────────┐  ┌──────────────┐
│ Playwright + stealth │──▶│ IP screen (drop  │──▶│ keep the joke,   │─▶│ dryrun / fal  │─▶│ run folder:  │
│ singular stickers,   │   │ ©/™) then rank   │   │ original wording │  │ / openai /    │  │ report + per │
│ sales signals        │   │ by "doing well"  │   │ + image prompt   │  │ higgsfield    │  │ design files │
└─────────────────────┘   └──────────────────┘   └──────────────────┘  └───────────────┘  └──────────────┘
```

Run it once with zero setup to see the whole thing work:

```bash
cd sticker-recreator
python3 run.py --source seed --provider dryrun
# -> writes output/run_<timestamp>/  (a full example is committed at sample-output/)
```

Open `sample-output/report.md` for the ranked list of recreations and the
listings that got dropped for IP risk.

---

## Live scraping (your machine, with the browser tricks)

The seed path needs nothing. Live scraping needs Playwright:

```bash
pip install -r requirements.txt
playwright install chromium          # skip on the Claude web image — Chromium is pre-installed

python3 run.py --source etsy   --query "funny camping sticker" --limit 30 --top 12
python3 run.py --source amazon --query "hiking sticker"        --limit 30 --top 12
python3 run.py --source both   --query "national park sticker" --provider dryrun
```

**Getting past the bot blocking** (Etsy = PerimeterX, Amazon = "Robot Check").
`sticker_recreator/browser.py` is the stealth layer:

- drives real Chromium with `AutomationControlled` disabled (not the headless shell);
- patches the automation tells (`navigator.webdriver`, plugins, `window.chrome`, WebGL);
- rotates a realistic UA / viewport / locale / timezone;
- humanizes with mouse moves, scroll and jittered waits;
- detects challenge pages and raises instead of hammering.

The single biggest lever is a **residential / rotating proxy** — datacenter IPs
get blocked fast. Set it and go:

```bash
export SCRAPER_PROXY="http://user:pass@residential-proxy:port"
export SCRAPER_HEADLESS=0            # watch the browser while you tune
python3 run.py --source etsy --query "camping sticker"
```

> Selectors change. Etsy is read via JSON-LD (stable); Amazon via DOM selectors
> grouped at the top of `scrape_amazon.py`. If a site refactors, adjust those in
> one place. Scrape gently and within each site's terms — this is tuned for
> low-volume market research, not bulk harvesting.

---

## Wiring the image generator later

The generator is one function with swappable providers. Export a token and flip
`--provider`:

```bash
export FAL_KEY=...                    # matches the shop's existing fal stack
python3 run.py --source seed --provider fal

# or
export OPENAI_API_KEY=...
python3 run.py --source seed --provider openai
```

| provider     | needs            | writes        | notes |
|--------------|------------------|---------------|-------|
| `dryrun`     | nothing          | `design.svg`  | default; placeholder mockup |
| `fal`        | `FAL_KEY`        | `design.png`  | `FAL_MODEL` default `fal-ai/flux/schnell` |
| `openai`     | `OPENAI_API_KEY` | `design.png`  | `gpt-image-1` |
| `higgsfield` | —                | —             | hook; call it from the n8n Higgsfield node, or add the REST call |

Every provider gets the same `image_prompt` + `negative_prompt` from the brief,
so switching providers doesn't change the creative direction.

---

## The copyright / trademark line (why this is safe)

The whole design rests on one rule: **a joke/idea isn't copyrightable — the
specific artwork and exact wording are.** So the pipeline *keeps the humour* and
makes the *expression* original.

- `ip_filter.py` + `data/ip_blocklist.json` score every listing for protected IP
  (characters, brands, celebrities, registered slogans, "officially licensed"
  language). High risk → **dropped before recreation**, with the reason logged in
  `listings_blocked.json` and `report.md`.
- Every brief carries a negative prompt and an explicit IP guardrail so the
  generated art stays free of logos, characters and lookalike type.
- The blocklist is intentionally conservative and easy to extend — add a term and
  it's enforced on the next run.

This is a competitive-research + original-design tool. It is **not** for copying a
competitor's artwork; the 85–90% target means "same joke, same shelf appeal,"
never "same file."

---

## Output folder layout

```
run_<timestamp>/
  report.md              ← ranked recreations + the IP-blocked list (start here)
  report.csv             ← same, for a spreadsheet / EverBee cross-check
  listings_raw.json      ← everything scraped
  listings_blocked.json  ← what the IP screen dropped, with reasons
  designs.json           ← full structured result
  designs/<slug>/
    brief.md             ← the joke, what to keep/change, copy options, art direction
    prompt.txt           ← paste into any image model
    negative_prompt.txt
    design.svg|png       ← the mockup (dryrun) or generated art (fal/openai)
    meta.json
```

---

## Hooking into n8n / Supabase (optional, matches the current stack)

This mirrors the **R Designs – Q Daily Scan** pattern (n8n schedules, a poller
does the heavy browser work):

1. **Schedule** — an n8n Schedule Trigger (or cron) runs `run.py` on the
   Docker/n8n host via an *Execute Command* / SSH node, e.g. daily 4:00 AM PT to
   match the existing scan window.
2. **Queue** — feed the ranked survivors into the existing "Design For Review"
   flow: write `designs.json` rows into the Supabase design queue with
   `status='Design For Review'`, `image_prompt`, `alt_copy`, and the source link,
   so the VA reviews recreations the same way they review Q designs today.
3. **Generate** — either let this tool call fal directly (`--provider fal`) or
   leave `dryrun` and let the existing carousel/mockup pipelines render the
   approved copy.

`designs.json` is already shaped for that handoff — one object per design with
`brief.image_prompt`, `brief.alt_copy`, `listing.url` and the perf/IP scores.

---

## Tuning

| flag / env | what it does |
|---|---|
| `--source` | `seed` \| `etsy` \| `amazon` \| `both` |
| `--query` | search phrase (required for live scraping) |
| `--limit` | max listings to scrape per source |
| `--top` | how many top performers to recreate |
| `--min-perf` | drop anything below this 0–1 performance score |
| `--include-multipacks` | keep packs/sheets (default: singular stickers only) |
| `SCRAPER_PROXY` | residential/rotating proxy — the main anti-block lever |
| `SCRAPER_HEADLESS=0` | watch the browser to debug blocks |

Weights for "doing well" live in `performance.py`; the IP blocklist in
`data/ip_blocklist.json`; copy paraphrase rules and art-direction pool in
`brief.py`.
