# PETROVA — 1st-Line Operations Handbook
_For on-call / NOC / 1st-line support. The one question this answers: "An alert fired — what do I do?"_

## 1. Your role (1st-line)
**Watch → Acknowledge → Triage → Runbook → Escalate.**
You keep the platform healthy and communicate. You do **not** fix code or redesign — that's L2/L3. Your job is fast acknowledgement, known-issue resolution, and clean escalation.

## 2. What you watch (single pane of glass)
| Tool | Shows | "Is it healthy?" |
|---|---|---|
| **Airflow UI** | pipeline runs, task status | all tasks green; last run succeeded |
| **Grafana** (Prometheus) | system metrics: CPU/mem/latency | within Golden-Signal bands |
| **Kibana** (ELK) | logs across all systems | no error spikes; search by `correlation_id` |
| **Streamlit / Power BI** | business KPIs + SPC alerts | dashboard live, data fresh |
**Golden Signals to scan:** latency · errors · traffic · saturation.

## 3. Severity tiers + your action
| Severity | Means | Your action | Respond within |
|---|---|---|---|
| 🔴 **CRITICAL** | prod/revenue impact (SPC UNSTABLE+NOISY, Gold-gate CRITICAL, pipeline down) | Acknowledge → follow runbook → page L2 on-call if not resolved | **15 min** |
| 🟠 **WARNING** | degraded (single gate fail, SPC warning, SLA near-miss) | Investigate, log it, escalate if it persists | same shift |
| 🟢 **OK** | normal | dashboard only, no action | — |

## 4. The 5-minute triage flow (do this for ANY alert)
```
1. Acknowledge the alert (PagerDuty/email) — stops re-paging
2. Open the dashboard (Airflow / Grafana / Kibana)
3. Grab the correlation_id / run_id from the alert
4. Search ELK by that id → find the root-cause log
5. Match to the playbook below
6. KNOWN issue → run the fix.   UNKNOWN/complex → escalate to L2.
```

## 5. Alert response playbook (the top ones)
| Alert | Likely cause | 1st-line action | Escalate to L2 if |
|---|---|---|---|
| **Pipeline failed** (Airflow task) | transient error | Check task log → **retry once** (Airflow UI) | retry fails |
| **Bronze gate fail** | bad source data | Check `quarantine_bronze`, notify source owner | data issue persists |
| **Silver gate fail** | quality < threshold | Review `quarantine_silver` records | looks like a logic bug |
| **Gold gate CRITICAL (SPC)** | equipment anomaly (e.g. K-201) | Notify reliability/ops team; verify the sensor | confirmed real fault |
| **SLA miss** | slow / heavy run | Check for a slow model or resource cap | repeated misses |
| **Dashboard down** | Streamlit/app sleeping | Confirm Zero-Copy-Clone fallback is serving; wake app | down > 15 min |
| **Snowflake "warehouse suspended"** | auto-suspend | runs auto-resume; if stuck, `ALTER WAREHOUSE … RESUME` | persists |

## 6. Escalation matrix
```
L1 (you)     → watch · acknowledge · runbook · communicate
   ↓ (if not a known fix, or > SLA)
L2 Data Eng  → fix code / data / pipeline logic
   ↓
L3 Architect → permanent design fix (the "prevent" step)
```
**Rule:** CRITICAL unresolved after 15 min → page L2. Always post a status update, even "still investigating."

## 7. Quick reference (fill in your URLs)
- Airflow UI: `http://<host>:8080`  ·  Kibana: `<elk-url>`  ·  Grafana: `<grafana-url>`
- Re-run a DAG: Airflow UI → DAG → **Trigger DAG**
- K8s (if used): `kubectl get pods` · `kubectl logs <pod> --previous` · `kubectl describe pod <pod>`
- Snowflake resume: `ALTER WAREHOUSE PETROVA_WH RESUME;`

## 8. Shift checklist
**Start of shift:** dashboards green? · any open CRITICAL? · read the handover note.
**End of shift:** log open issues · write handover · update the incident log.

## 9. Related docs
`DASHBOARD_GUIDE.md` (dashboard detail) · `runbooks/incident_response.md` · `runbooks/k8s_incident_playbook.md` · `END_TO_END_WORKFLOW.md`

## 🥇 The golden rule
> **Acknowledge fast · communicate always · escalate when unsure. Available beats perfect — but never silent.**
