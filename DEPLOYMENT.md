# Deploying These Workflows To Docker n8n

## What we want

Use GitHub as the source of truth for workflow JSON exports so changes can be made in code, pushed to `main`, and automatically synced into the Docker-hosted n8n instance.

## Chosen deployment architecture

This repo is set up for:

- GitHub Actions
- a self-hosted GitHub runner on the Docker/n8n machine
- n8n API key authentication
- manifest-based workflow mapping

Files involved:

- deploy workflow:
  [ .github/workflows/deploy-n8n.yml ](/Users/davidtrinh/Documents/Playground/mildly-outdoorsy-n8n-workflows/.github/workflows/deploy-n8n.yml)
- sync script:
  [ scripts/deploy_workflows_to_n8n.py ](/Users/davidtrinh/Documents/Playground/mildly-outdoorsy-n8n-workflows/scripts/deploy_workflows_to_n8n.py)
- manifest:
  [ workflow-manifest.json ](/Users/davidtrinh/Documents/Playground/mildly-outdoorsy-n8n-workflows/workflow-manifest.json)

## How deployment works

1. Workflow JSON changes are committed and pushed to `main`.
2. GitHub Actions runs on the self-hosted runner.
3. The runner calls the live n8n API using:
   - `N8N_BASE_URL`
   - `N8N_API_KEY`
4. Each workflow is resolved by:
   - `n8n_workflow_id` when present
   - otherwise exact `live_name`
5. The live workflow is updated in place and set active/inactive according to the manifest.

## What you still need to configure

### 1. Install a self-hosted GitHub runner

Install the runner on the same machine that hosts Docker/n8n.

GitHub path:

- repo -> `Settings -> Actions -> Runners -> New self-hosted runner`

Use the default `self-hosted` label unless you want custom labels later.

### 2. Create an n8n API key

In the live n8n instance, create an API key and save it in GitHub repo secrets as:

- `N8N_API_KEY`

### 3. Set the live n8n base URL

Add this GitHub repo secret:

- `N8N_BASE_URL`

Examples:

- `http://127.0.0.1:5678`
- `https://your-n8n.example.com`

If the runner is on the same machine as Docker/n8n, `http://127.0.0.1:5678` is usually the cleanest option.

### 4. Fill workflow IDs in the manifest

Open each workflow in n8n and copy the workflow ID into:

[workflow-manifest.json](/Users/davidtrinh/Documents/Playground/mildly-outdoorsy-n8n-workflows/workflow-manifest.json)

The script can resolve by exact `live_name`, but explicit workflow IDs are safer and more stable.

## First-time validation

After the runner and secrets are ready:

1. trigger the GitHub Action manually
2. it will run a dry run first
3. then it will sync the live workflows

GitHub path:

- repo -> `Actions -> Deploy n8n workflows -> Run workflow`

## Important note about credentials

Workflow JSON exports are not the credential source of truth.

Live n8n must already contain working credentials for:

- Supabase
- Placid
- Instagram publishing
- fal.ai
- NewsAPI

This deployment only updates workflows, not credentials.
