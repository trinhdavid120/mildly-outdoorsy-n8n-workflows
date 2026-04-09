# Mildly Outdoorsy n8n Workflows

This folder contains the exported n8n workflows that drive the `Mildly Outdoorsy` Instagram automation stack.

## What is here

- `Mildly Outdoorsy - Carousel Pipeline - SAFE #6 (1).json`
  - Product carousel generation and publishing flow.
- `Mildly Outdoorsy - NewsAPI Ingest - SAFE #6.json`
  - Finds outdoors-relevant stories and inserts pending queue rows.
- `Mildly Outdoorsy - Twitter Card Pipeline - SAFE #6 (1).json`
  - Renders and publishes pending `twitter_card_balanced` queue rows.
- `Mildly Outdoorsy - Snap Overlay Pipeline - SAFE #6.json`
  - Renders and publishes pending `snap_overlay` queue rows.
- `Mildly Outdoorsy - Shared IG Publisher - SAFE #6.json`
  - Shared publishing workflow used by the content-specific pipelines.

## Current architecture

- `NewsAPI Ingest -> Twitter Card Pipeline -> Shared IG Publisher`
- `Snap Overlay Pipeline -> Shared IG Publisher`
- `Carousel Pipeline -> Instagram directly` for carousel item/container/publish inside the workflow export

## Goal for version control

The intention is to keep this folder as the Git/GitHub source of truth for exported workflow JSON files so future edits can be:

1. made here
2. committed to GitHub
3. re-imported or synced into the Docker-hosted n8n instance

## Recommended repo strategy

This `Playground` directory already has a top-level `.git`, so the safest long-term move is:

1. create a dedicated GitHub repo for just these n8n workflow files
2. either move/copy this folder into that repo, or make this folder its own nested repo if you prefer
3. keep only workflow exports and deployment docs in that repo

## Next deployment step

See [DEPLOYMENT.md](/Users/davidtrinh/Documents/Playground/n8n%20files/DEPLOYMENT.md) for the cleanest GitHub + Docker n8n path.
