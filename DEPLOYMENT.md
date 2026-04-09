# Deploying These Workflows To Docker n8n

## What we want

Use GitHub as the source of truth for workflow JSON exports so changes can be made in code, reviewed, pushed, and then deployed into the Docker-hosted n8n instance.

## Practical deployment options

### Option A: Manual import after pull

Simplest setup.

1. Commit workflow JSON changes to GitHub.
2. Pull latest repo on the machine running Docker n8n.
3. In the n8n UI, import the updated workflow JSON files.

Good for:

- low risk
- infrequent workflow edits
- keeping human review in the loop

### Option B: n8n API-driven sync

Better for automation.

1. Commit workflow JSON changes to GitHub.
2. A deploy script or GitHub Action calls the n8n API.
3. The API updates existing workflows by workflow ID.

This is the better path if you want Codex to edit workflow files and have a predictable deployment story.

What is needed:

- n8n base URL
- n8n API key
- workflow ID mapping for each JSON export

## Recommended structure

Keep these values outside version control:

- n8n base URL
- n8n API key
- Instagram token
- Supabase credentials
- Placid credentials
- fal.ai key

Keep these in version control:

- exported workflow JSON files
- deployment docs
- workflow manifest

## Suggested deploy flow

1. Edit workflow JSON in GitHub-tracked repo.
2. Validate JSON shape before deploy.
3. Push branch.
4. Merge to `main`.
5. On the Docker/n8n machine:
   - pull latest repo
   - import updated workflow JSONs, or
   - run a future API sync script

## Important note about credentials

Workflow JSON exports should not be treated as the credential source of truth. Credentials belong in the live n8n instance. After importing workflows into a fresh Docker n8n environment, verify credential bindings for:

- Supabase
- Placid
- Instagram publishing
- fal.ai / NewsAPI / any HTTP auth nodes

## What Codex can realistically manage well

Codex is a strong fit for:

- editing workflow JSON files
- cleaning broken node wiring
- updating text prompts and scheduling logic
- maintaining docs and manifests
- preparing API-sync scripts

Codex will work best if you provide either:

- access to the GitHub repo plus exported JSON files, or
- direct access to the Docker host / n8n API

## Recommended next implementation

Create a workflow manifest mapping logical workflow names to filenames and live n8n workflow IDs, then add an import/sync script later.
