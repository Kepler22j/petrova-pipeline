# PETROVA Pipeline — Session Command Log & 30-Hour Study Plan

> **Purpose:** Interview-ready reference showing exactly what I did, why, and how I troubleshot real-world DevOps + Data Engineering problems across Git, Snowflake, Databricks, and dbt.

---

## Part 1: PowerShell Command Log (What I Did & Why)

### 1.1 Git Authentication & Token Management

| # | Command | Purpose | Reason / Problem | Result |
|---|---------|---------|-------------------|--------|
| 1 | `git push origin main` | Push new code to GitHub | Needed to push certification lab guides to remote repo | **FAILED** — `Authentication failed. Password authentication is not supported` |
| 2 | `gh auth login` | Try GitHub CLI authentication | Attempted alternative auth method | **FAILED** — `gh is not recognized` (GitHub CLI not installed on Windows) |
| 3 | `git credential-manager erase` | Clear cached credentials | Old/expired token cached in Windows Credential Manager | **FAILED** — `Missing 'protocol' input argument` (wrong syntax) |
| 4 | `cmdkey /delete:git:https://github.com` | Delete stored credential via Windows API | Another attempt to clear cached credentials | **FAILED** — `Element not found` (credential stored under different key) |
| 5 | `git remote set-url origin https://Kepler22j:ghp_aDA4...@github.com/Kepler22j/petrova-pipeline.git` | Set remote URL with new Personal Access Token | Generated a new PAT from GitHub Settings > Developer Settings > Tokens | **FAILED** — `403 Permission denied` because token had **public access scope only**, not `repo` scope |
| 6 | *(GitHub UI)* Generated new token (classic) with **`repo` scope** checked | Create token with correct permissions | Previous token lacked write access to repositories — the `repo` scope grants full control of private repos | **SUCCESS** — Token `ghp_IfFRK...` generated with repo scope, expires Aug 26 2026 |
| 7 | `git remote set-url origin https://Kepler22j:ghp_IfFRK...@github.com/Kepler22j/petrova-pipeline.git` | Update remote URL with correct token | Replace the insufficient-scope token with the new repo-scoped one | **SUCCESS** — Remote URL updated |
| 8 | `git push origin main` | Push to GitHub | Retry with correct token | **SUCCESS** — `Everything up-to-date` (files were already committed) |

**Interview Talking Point:**
> "I hit a classic PAT scope issue — my first token only had `public_repo` access, which can't push to private repos. The fix was generating a new token (classic) with the full `repo` scope. This is a common gotcha when GitHub deprecated password auth in 2021 — you need to understand OAuth scopes to troubleshoot push failures."

---

### 1.2 Git File Operations & Troubleshooting

| # | Command | Purpose | Reason / Problem | Result |
|---|---------|---------|-------------------|--------|
| 9 | `git status` | Check working tree state | Verify which files need staging | Showed `nothing to commit, working tree clean` — files were already committed in a prior operation |
| 10 | `git add docs/upskill/` | Stage new lab guide files | 3 new .md files created in docs/upskill/ | No output (already tracked) |
| 11 | `git commit -m "docs: add certification lab guides (Snowflake, Databricks, dbt)"` | Commit with descriptive message | Following conventional commit format (`docs:` prefix) | `nothing to commit` — already committed |
| 12 | `git ls-files "databricks/notebooks/Jay_Agent*"` | Check if broken directory is tracked in git | `git status` warned: `could not open directory 'databricks/notebooks/Jay_Agent. /'` | Empty output — not tracked in git, just a stale local directory with invalid name (trailing dot+space) |
| 13 | `git ls-files docs/upskill/` | Verify lab guides are tracked in repo | Confirm all 3 files made it into git | **SUCCESS** — listed all 3: `databricks_lab_guide.md`, `dbt_lab_guide.md`, `snowflake_lab_guide.md` |
| 14 | `Remove-Item "...\snowflake_lab_guide.md.bak"` | Delete backup file | Cleanup artifact from file editing | **SUCCESS** — .bak file removed |
| 15 | `Remove-Item "...\index.lock"` | Remove stale git lock file | Git operations failed with `Unable to create index.lock: File exists` | `does not exist` — lock was already cleared |

**Interview Talking Point:**
> "I used `git ls-files` to verify tracked files without relying on `git status` alone — useful when status shows 'clean' but you want to confirm specific paths are in the index. The `Jay_Agent.` warning was a Windows filesystem edge case — NTFS doesn't allow trailing dots in directory names, but git can track them from Linux/Mac commits."

---

### 1.3 File Management & Project Organization

| # | Command | Purpose | Reason / Problem | Result |
|---|---------|---------|-------------------|--------|
| 16 | `cd C:\Users\keplor22J\projects\petrova-pipeline` | Navigate to project root | PowerShell opens in system32 by default | Changed to project directory |
| 17 | `dir docs\upskill\` | List files in upskill directory | Verify lab guide files exist on disk | Showed 3 .md files + 1 .bak file |
| 18 | `Copy-Item` (from outputs to project) | Copy generated files to project folder | Files created in sandbox needed to be moved to the mounted project directory | **SUCCESS** — 3 lab guides copied to `docs/upskill/` |

---

## Part 2: Three-Pillar Troubleshooting Journey

### 2.1 Snowflake (fvakveg-xt38879.snowflakecomputing.com)

| Issue | What Happened | How I Fixed It | Skill Demonstrated |
|-------|--------------|----------------|-------------------|
| Account login | Used `etlpetrova097@gmail.com` to access trial account | Configured account identifier format: `fvakveg-xt38879` | Snowflake account architecture (org.account) |
| Warehouse setup | Needed XS warehouse with auto-suspend for cost control | Created via DDL: `AUTO_SUSPEND = 300, AUTO_RESUME = TRUE, INITIALLY_SUSPENDED = TRUE` | Resource management, cost optimization |
| RBAC hierarchy | 4-role model needed proper grant chains | `ACCOUNTADMIN > SYSADMIN > PETROVA_ENGINEER > PETROVA_READER` with future grants | Security, principle of least privilege |
| Data sharing & masking | Sensitive vendor data needed protection | Dynamic data masking policies + row access policies | Data governance, compliance |

### 2.2 Databricks (adb-7405607079686031.11.azuredatabricks.net)

| Issue | What Happened | How I Fixed It | Skill Demonstrated |
|-------|--------------|----------------|-------------------|
| Workspace provisioning | Azure East Asia region, Premium tier | Deployed via Azure Portal in `rg-petrova-dev` resource group | Cloud resource provisioning |
| Unity Catalog | 3-level namespace setup for data governance | `catalog.schema.table` with managed + external locations | Data mesh, governance |
| Auto Loader | Schema evolution on streaming ingests | `cloudFiles.schemaLocation` + `mergeSchema` option | Schema-on-read, streaming |
| Delta Lake | ACID transactions, time travel, optimization | `OPTIMIZE`, `VACUUM`, `Z-ORDER` on high-cardinality columns | Storage optimization, lakehouse |

### 2.3 dbt (16 models, 51 tests)

| Issue | What Happened | How I Fixed It | Skill Demonstrated |
|-------|--------------|----------------|-------------------|
| SCD Type 2 | Vendor dimension needed historical tracking | `incremental` materialization + `row_hash` comparison + post_hook UPDATE | Slowly changing dimensions, data modeling |
| Sensor alerts | 6 alert categories from raw sensor data | Statistical Process Control: stddev, LAG, coefficient of variation | Analytics engineering, domain modeling |
| Test pyramid | 4-level testing strategy | L1 Unit (seeds) → L2 Integration (Docker) → L3 Load (10GB) → L4 Chaos (8 scenarios) | Quality engineering, test architecture |
| Macros/Jinja | Reusable SQL patterns | Custom macros + `dbt_utils` package for surrogate keys, freshness | DRY principles, code reuse |

---

## Part 3: 30 Certification Lab Tasks (Notion Tracked)

### Snowflake SnowPro Core (COF-C03) — 10 Labs

| Lab | Task | Exam Domain | Est. Hours |
|-----|------|-------------|------------|
| 1 | Warehouse Sizing & Multi-Cluster Config | Performance & Tuning (18%) | 1.5 |
| 2 | Database & Schema Architecture (Medallion) | Data Architecture (25%) | 1.0 |
| 3 | Data Loading: COPY INTO, Snowpipe, Stages | Data Loading & Transformation (20%) | 2.0 |
| 4 | Semi-Structured: VARIANT, FLATTEN, PARSE_JSON | Data Transformation (20%) | 2.0 |
| 5 | Time Travel & Zero-Copy Clone | Data Protection (10%) | 1.5 |
| 6 | Streams & Tasks (CDC Pipelines) | Data Transformation (20%) | 2.0 |
| 7 | RBAC: Roles, Grants, Future Grants | Security (15%) | 1.5 |
| 8 | Data Sharing & Dynamic Masking | Data Sharing (10%) | 1.5 |
| 9 | Resource Monitors & Cost Control | Performance & Tuning (18%) | 1.0 |
| 10 | Mock Exam & Review | All Domains | 1.5 |
| | | **Subtotal** | **15.5** |

### Databricks Data Engineer Associate — 10 Labs

| Lab | Task | Exam Domain | Est. Hours |
|-----|------|-------------|------------|
| 11 | Workspace, Clusters & Notebooks | Databricks Lakehouse (24%) | 1.5 |
| 12 | DataFrame API & Spark SQL | ELT with Spark SQL (29%) | 2.0 |
| 13 | Delta Lake: CRUD, Time Travel, DESCRIBE HISTORY | Databricks Lakehouse (24%) | 2.0 |
| 14 | Auto Loader & COPY INTO | Incremental Processing (22%) | 2.0 |
| 15 | Structured Streaming: Triggers & Watermarks | Incremental Processing (22%) | 2.0 |
| 16 | OPTIMIZE, VACUUM, Z-ORDER | Databricks Lakehouse (24%) | 1.5 |
| 17 | Delta Live Tables (DLT) Pipelines | Production Pipelines (16%) | 1.5 |
| 18 | Unity Catalog & Data Governance | Databricks Lakehouse (24%) | 1.5 |
| 19 | Jobs, Workflows & Orchestration | Production Pipelines (16%) | 1.5 |
| 20 | Mock Exam & Review | All Domains | 1.5 |
| | | **Subtotal** | **17.0** |

### dbt Analytics Engineer — 10 Labs

| Lab | Task | Exam Domain | Est. Hours |
|-----|------|-------------|------------|
| 21 | Project Structure & dbt init | dbt Fundamentals (15%) | 1.0 |
| 22 | Materializations: view, table, incremental, ephemeral | Materializations (15%) | 1.5 |
| 23 | Sources, refs & Documentation | Sources & Testing (20%) | 1.5 |
| 24 | Testing: schema, data, custom, packages | Sources & Testing (20%) | 2.0 |
| 25 | Incremental Models & SCD Type 2 | Materializations (15%) | 2.0 |
| 26 | Seeds, Snapshots & Source Freshness | Sources & Testing (20%) | 1.5 |
| 27 | Macros, Jinja & Custom Schemas | Jinja & Macros (10%) | 2.0 |
| 28 | Packages: dbt_utils, codegen, expectations | Implementation (20%) | 1.5 |
| 29 | dbt Cloud IDE, Jobs & CI/CD | Deployment (15%) | 1.5 |
| 30 | Mock Exam & Review | All Domains | 1.5 |
| | | **Subtotal** | **16.0** |

| Platform | Hours | Status |
|----------|-------|--------|
| Snowflake | 15.5 | Not Started |
| Databricks | 17.0 | Not Started |
| dbt | 16.0 | Not Started |
| **Total** | **48.5** | — |

> **Note:** 48.5 hours total. At ~5 hrs/day across 7 days = 35 hours this week. Prioritize to fit 30 hours — see schedule below.

---

## Part 4: 30-Hour Weekly Study Plan (This Week)

### Strategy: Focus on highest exam-weight domains first, skip mock exams for now

| Day | Platform | Labs | Focus | Hours |
|-----|----------|------|-------|-------|
| **Fri (Today)** | Snowflake | Lab 1-2 | Warehouses + Schemas (foundation) | 2.5 |
| **Sat** | Snowflake | Lab 3-4 | Data Loading + Semi-Structured (40% exam weight) | 4.0 |
| **Sun** | Snowflake | Lab 5-7 | Time Travel + Streams + RBAC | 5.0 |
| **Mon** | Databricks | Lab 11-13 | Workspace + DataFrames + Delta Lake | 5.5 |
| **Tue** | Databricks | Lab 14-16 | Auto Loader + Streaming + Optimization | 5.5 |
| **Wed** | dbt | Lab 21-25 | Core dbt: structure → incremental → SCD2 | 8.0 |
| **Thu** | dbt | Lab 26-28 | Seeds + Macros + Packages | 5.0 |
| | | | **Week Total** | **35.5** |

### What gets deferred to next week (remaining ~13 hours):
- Snowflake Labs 8-10 (Data Sharing, Cost Control, Mock Exam)
- Databricks Labs 17-20 (DLT, Unity Catalog, Jobs, Mock Exam)
- dbt Labs 29-30 (Cloud IDE, Mock Exam)

---

## Part 5: How to Track Progress in Notion

All 30 tasks are already in your Notion task database. To track them:

1. **Open Notion Hub:** https://www.notion.so/36dd2fa90d95817eae20c97d53357f05
2. **Filter by tag:** Use the Category/Tag filter to show only certification tasks
3. **Update status** as you complete each lab:
   - `Not Started` → `In Progress` → `Done`
4. **Icons by platform:**
   - Snowflake tasks have snowflake icons
   - Databricks tasks have spark/fire icons
   - dbt tasks have gear/wrench icons

---

## Part 6: Interview Script — How to Explain This Experience

### Q: "Walk me through your PETROVA pipeline project."

> "PETROVA is a production-grade hybrid cloud data platform I built end-to-end across three pillars — Snowflake for cloud warehousing, Databricks for lakehouse processing, and dbt for transformation orchestration.
>
> On the **infrastructure side**, I set up a medallion architecture (Bronze → Silver → Gold) with SCD Type 2 tracking on vendor dimensions using dbt incremental models with row-hash change detection. I built a sensor alert engine using Statistical Process Control — six alert types derived from three statistical primitives: standard deviation, LAG, and business thresholds.
>
> On the **DevOps side**, I managed the full Git workflow — and actually hit a real-world PAT scope issue where my GitHub token had `public_repo` access only, causing 403 errors on push. I debugged it by checking the token scopes, regenerating with the `repo` scope, and updating the remote URL. I also dealt with stale `index.lock` files and Windows filesystem edge cases with invalid directory names from cross-platform commits.
>
> For **quality**, I implemented a 4-level test pyramid: unit tests with dbt seeds, integration tests in Docker, load tests at 10GB scale on Databricks, and chaos tests covering 8 failure scenarios like network timeouts, schema drift, and null floods.
>
> I tracked all certification lab work in Notion — 30 structured labs across the three platforms mapped to exam domains, prioritized by exam weight percentage."

### Q: "What was the hardest troubleshooting problem?"

> "The GitHub authentication chain was interesting because it had multiple failure modes stacked on top of each other. First, `gh auth login` failed because GitHub CLI wasn't installed. Then `git credential-manager erase` failed because of wrong syntax. Then `cmdkey /delete` failed because the credential was stored under a different key. Finally, I generated a new PAT but it still returned 403 — because I'd only checked 'public access' instead of the 'repo' scope. Each failure taught me something about Windows credential management and GitHub's OAuth scope model."

---

*Generated: 2026-05-29 | PETROVA Pipeline Project | Jay (Ghost in the Shell)*
