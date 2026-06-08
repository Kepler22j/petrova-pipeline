# 🚨 Kubernetes Incident Playbook (SRE) — Symptom → Diagnose → Fix → Prevent
_Study + interview demo doc for the Digile System Engineer role. SRE = detect → diagnose → fix → prevent. Tie every one to your 8D / 5-Whys RCA._

## How to use in an interview
For ANY "how would you handle X" → answer in this order: **1) Symptom seen → 2) Diagnose (the command) → 3) Fix → 4) Prevent (permanent guard).** That structure alone signals senior SRE.
> *"I read the events and logs, root-cause with 5-Whys, fix, then add a permanent guard — a probe, a limit, or an alert."*

---

## 🔴 Category 1 — Scaling / Load
**1. Traffic spike → overload**
- Symptom: latency/errors rise under load · Diagnose: `kubectl top pods`, HPA status `kubectl get hpa`
- Fix: **HPA** scales replicas out · Prevent: set HPA min/max + realistic CPU target (e.g., 70%)

**2. CPU starvation (pods exist but maxed)**
- Symptom: high latency, CPU at limit · Diagnose: `kubectl top pods`, `describe` (throttling)
- Fix: raise CPU `requests/limits` + HPA · Prevent: load-test to size requests correctly

**3. Memory leak → OOMKilled (restart loop)**
- Symptom: pods restart; reason `OOMKilled` · Diagnose: `kubectl describe pod` (Last State: OOMKilled)
- Fix: raise memory limit (mitigation) · Prevent: **fix the leak** + memory limit + alert on restarts

## 🔴 Category 2 — Crash / Failure
**4. CrashLoopBackOff (app keeps crashing)**
- Symptom: status `CrashLoopBackOff` · Diagnose: `kubectl logs <pod> --previous` + `kubectl describe pod` (events)
- Fix: fix bug/config → redeploy · Prevent: CI test + readiness probe + staging

**5. ImagePullBackOff (can't pull image)** ← *commonly missed*
- Symptom: status `ImagePullBackOff`/`ErrImagePull` · Diagnose: `kubectl describe pod` (image name / registry auth)
- Fix: correct image tag / add `imagePullSecret` · Prevent: pin tags, validate registry creds in CI

**6. Dependency failure (DB/API down)**
- Symptom: app healthy but failing calls · Diagnose: `kubectl logs`, test the dependency
- Fix: retry/backoff + circuit breaker · Prevent: **readiness probe** (stop routing until deps OK) + alerts

**7. Pod stuck Pending (unschedulable)**
- Symptom: status `Pending` · Diagnose: `kubectl describe pod` → scheduler events
- Cause/Fix: no node resources (→ cluster autoscaler), taints/affinity, unbound PVC · Prevent: right-size requests, autoscaler

**8. Liveness probe misconfigured → restart loop** ← *probes can CAUSE outages*
- Symptom: healthy app keeps restarting · Diagnose: `kubectl describe pod` (Liveness failed events)
- Fix: correct probe path/port/timeouts, add `initialDelaySeconds` · Prevent: test probes in staging

## 🔴 Category 3 — Networking
**9. Service not reachable**
- Symptom: can't hit the service · Diagnose: `kubectl get endpoints <svc>` (empty = selector mismatch), `kubectl get svc`
- Fix: align Service `selector` with pod labels · Prevent: label conventions + readiness gating

**10. Pod-to-pod communication failure**
- Symptom: internal calls fail · Diagnose: `kubectl exec` + curl/nslookup; check NetworkPolicy
- Fix: correct Service DNS / NetworkPolicy · Prevent: explicit, tested network policies

## 🔴 Category 4 — Deployment / CI-CD
**11. Broken deployment (bad version)**
- Symptom: errors right after deploy · Diagnose: `kubectl rollout status`, `logs`
- Fix: **`kubectl rollout undo deployment/<name>`** (instant rollback) · Prevent: canary/blue-green + smoke tests

**12. Partial rollout failure (some pods new, some old)**
- Symptom: mixed behavior during deploy · Diagnose: `kubectl get pods` (image versions), `rollout status`
- Fix: rolling-update strategy + **readiness probes gate the rollout** · Prevent: `maxUnavailable/maxSurge` tuned

## 🔴 Category 5 — Observability
**13. No logs / no visibility**
- Symptom: failing but blind · Diagnose: `kubectl logs`, then centralize → **ELK**
- Fix: ship logs (Filebeat→Elasticsearch→Kibana) · Prevent: structured logging + correlation IDs *(your strength)*

**14. Alerts not firing (silent failure)**
- Symptom: incident found late · Diagnose: check Prometheus rules / Alertmanager
- Fix: add/repair **Prometheus alert rules** · Prevent: alert on Golden Signals (latency, errors, traffic, saturation)

## 🔴 Category 6 — Configuration
**15. Wrong config (env var)**
- Symptom: app fails immediately on start · Diagnose: `kubectl logs`, `kubectl get configmap/secret -o yaml`
- Fix: correct **ConfigMap/Secret** → restart · Prevent: validate config in CI, schema-check

**16. Secret leakage / exposed credentials**
- Symptom: creds in logs/repo/image · Diagnose: scan repo & images · Fix: rotate + move to **Secret/Vault**
- Prevent: never hardcode, gitignore `.env`, secret scanning *(you did exactly this with the GitHub token)*

## 🔴 Category 7 — Cluster Level
**17. Node failure (machine down)**
- Symptom: node `NotReady`, pods evicted · Diagnose: `kubectl get nodes`, `describe node`
- Fix: **K8s reschedules pods to healthy nodes** (this is why K8s exists) · Prevent: multi-node, pod anti-affinity, PodDisruptionBudget

---

## 🔁 Maps to your existing skills (intuition — NOT exact 1:1)
| Your world | K8s analogy | Note |
|---|---|---|
| Airflow retry / `on_failure` | Pod restart / restartPolicy | good analogy |
| dbt test fail | Job/pod fail | good |
| Zero-Copy Clone failover | multi-replica / reschedule | good |
| Prometheus/ELK alerting | same in K8s | ✅ direct transfer |
| 8D / 5-Whys RCA | incident triage above | ✅ your edge |
> ⚠️ Don't claim "SLA breach = HPA" or "trigger_rule = scheduler" in the interview — those are loose. HPA = *load* scaling; alerting = SLA; scheduler = pod placement.

## 🎯 Honest framing
- Your SRE **mindset** transfers ~directly (detect→diagnose→fix→prevent, monitoring, RCA).
- The K8s **mechanics** are new → do the hands-on (HPA demo + 5-day plan).
- Say: *"My incident discipline transfers; I'm building the Kubernetes mechanics hands-on."*
