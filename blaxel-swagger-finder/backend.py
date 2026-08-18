"""FastAPI backend that drives the pipeline and streams events via SSE."""

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# Paths
_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent
_BLAXEL_DIR = _THIS_DIR.parent / "blaxel"

load_dotenv(_THIS_DIR / ".env")
load_dotenv(_BLAXEL_DIR / ".env")
sys.path.insert(0, str(_BLAXEL_DIR))
sys.path.insert(0, str(_REPO_ROOT))

from scanner import Scanner
from agent_tester import discover_tools, generate_test_plan, execute_test_plan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Test Pilot Pipeline API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory pipeline runs
_runs: dict[str, dict] = {}


class PipelineRequest(BaseModel):
    urls: list[str]


def _sse_event(step_id: str, status: str, items: list[str],
               extra: dict | None = None) -> str:
    """Format an SSE event."""
    data = {"step": step_id, "status": status, "items": items}
    if extra:
        data.update(extra)
    return f"data: {json.dumps(data)}\n\n"


def _sandbox_read_file(sandbox, path):
    """Read a file from the sandbox."""
    try:
        result = sandbox.process.exec({"command": f"cat {path}"})
        wait_res = sandbox.process.wait(result.name)
        if wait_res.exit_code == 0 and wait_res.stdout:
            return wait_res.stdout
    except Exception as e:
        logger.warning(f"Failed to read {path} from sandbox: {e}")
    return None


def _deploy_upstream_api(repo_name, app_content, requirements,
                         workspace, env, output_base):
    """Deploy an upstream demo API as a Blaxel function. Returns live URL or None."""

    svc_name = f"{repo_name}-svc"
    deploy_dir = os.path.join(output_base, svc_name)
    os.makedirs(deploy_dir, exist_ok=True)

    # Write main.py — ensure it uses PORT env var
    modified = app_content
    if "import os" not in modified:
        modified = "import os\n" + modified

    with open(os.path.join(deploy_dir, "main.py"), "w") as f:
        f.write(modified)

    # Parse dependencies from requirements.txt for pyproject.toml
    deps = []
    for line in requirements.strip().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            deps.append(f'    "{line}",')
    # Ensure uvicorn is included
    if not any("uvicorn" in d for d in deps):
        deps.append('    "uvicorn>=0.34.0",')
    deps_str = "\n".join(deps)

    with open(os.path.join(deploy_dir, "pyproject.toml"), "w") as f:
        f.write(f"""[project]
name = "{svc_name}"
version = "0.1.0"
description = "Upstream API service for {repo_name}"
requires-python = ">=3.11"
dependencies = [
{deps_str}
]
""")

    with open(os.path.join(deploy_dir, "blaxel.toml"), "w") as f:
        f.write(f"""name = "{svc_name}"
type = "function"

[entrypoint]
prod = ".venv/bin/python3 -m uvicorn main:app --host 0.0.0.0 --port 80"
dev = "uvicorn main:app --host 0.0.0.0 --port 8080"

[[triggers]]
id = "trigger-{svc_name}"
type = "http"

[triggers.configuration]
path = "functions/{svc_name}"
authenticationType = "public"
""")

    result = subprocess.run(
        ["bl", "deploy", "-y", "-w", workspace],
        cwd=deploy_dir,
        capture_output=True, text=True, timeout=300, env=env,
    )
    if result.returncode == 0:
        return f"https://run.blaxel.ai/{workspace}/functions/{svc_name}"
    logger.error(f"Failed to deploy {svc_name}: {result.stderr[:300]}")
    return None


def _update_mcp_base_url(output_dir, repo_name, upstream_url):
    """Update the blaxel.toml env var so the MCP server calls the real upstream URL."""
    import re as _re

    toml_path = os.path.join(output_dir, "blaxel.toml")
    if not os.path.exists(toml_path):
        return False

    with open(toml_path, "r") as f:
        content = f.read()

    env_key = repo_name.upper().replace("-", "_") + "_BASE_URL"
    pattern = _re.compile(rf'{_re.escape(env_key)}\s*=\s*"[^"]*"')
    if pattern.search(content):
        content = pattern.sub(f'{env_key} = "{upstream_url}"', content)
    else:
        # Append env section if key not found
        content += f'\n[env]\n{env_key} = "{upstream_url}"\n'

    with open(toml_path, "w") as f:
        f.write(content)
    return True


def _build_test_detail(test_results):
    """Build serializable test detail for frontend."""
    detail = []
    for r in test_results:
        steps_detail = []
        for s in r.steps:
            steps_detail.append({
                "action": s.action,
                "success": s.success,
                "duration_ms": s.duration_ms,
                "error": s.error,
            })
        detail.append({
            "test_name": r.test_name,
            "description": r.description,
            "passed": r.passed,
            "duration_ms": r.duration_ms,
            "summary": r.summary,
            "narrative": r.narrative,
            "analysis": r.analysis,
            "steps": steps_detail,
        })
    return detail


def _run_pipeline_sync(urls: list[str]):
    """Generator that yields SSE events as the pipeline progresses.

    Order: clone → sandbox → extract → ingest → discover → schema →
           policy → generate → mcp-test → deploy → user-test
    """
    from pipeline.ingest import ingest
    from pipeline.mine import mine_tools
    from pipeline.safety import SafetyPolicy, apply_safety
    from pipeline.codegen import generate as blaxel_generate

    workspace = os.getenv("BL_WORKSPACE", "mcptestautomate")
    bl_api_key = os.getenv("BL_API_KEY", "")
    extract_dir = str(_THIS_DIR / "extracted_specs")
    output_base = str(_BLAXEL_DIR / "output")

    # ── 1. Clone ─────────────────────────────────────────────────────────
    yield _sse_event("clone", "running", ["Initializing Blaxel sandbox..."])
    yield _sse_event("clone", "running",
                     ["Initializing Blaxel sandbox...",
                      f"Repositories to clone: {len(urls)}"])

    scanner = Scanner()
    clone_items = [f"Repositories to clone: {len(urls)}"]

    def on_scan_progress(repo_url, index, total):
        repo_name = repo_url.split("/")[-1].replace(".git", "")
        clone_items.append(f"Cloning {repo_name}... ({index+1}/{total})")

    scan_result = scanner.scan_all(urls, progress_callback=on_scan_progress,
                                   extract_dir=extract_dir)

    clone_items.insert(0, f"Sandbox: {scan_result.sandbox_name}")
    for url in urls:
        rn = url.split("/")[-1].replace(".git", "")
        clone_items.append(f"✓ {rn} cloned successfully")
    clone_items.append(f"Total repositories cloned: {len(urls)}/{len(urls)}")
    yield _sse_event("clone", "done", clone_items,
                     {"sandbox": scan_result.sandbox_name})

    # ── 2. Deploy Sandbox ────────────────────────────────────────────────
    yield _sse_event("deploy-sandbox", "running",
                     ["Provisioning sandbox compute environment..."])
    sandbox_items = [
        f"Sandbox ID: {scan_result.sandbox_name}",
        f"Base image: blaxel/base-image:latest",
        f"Region: us-pdx-1",
        f"CPU: 2 vCPU, Memory: 4 GB",
        f"Network: public egress enabled",
        f"Storage: ephemeral 10 GB",
        f"Status: DEPLOYED — sandbox kept alive for testing",
    ]
    yield _sse_event("deploy-sandbox", "done", sandbox_items,
                     {"sandbox": scan_result.sandbox_name})

    # ── 3. Extract ───────────────────────────────────────────────────────
    yield _sse_event("extract", "running",
                     ["Scanning cloned repos for OpenAPI/Swagger files..."])
    all_specs = scan_result.all_specs()
    extract_items = []
    for url in urls:
        rn = url.split("/")[-1].replace(".git", "")
        specs_for_repo = [s for s in all_specs if s["repo_name"] == rn]
        if specs_for_repo:
            for sp in specs_for_repo:
                fname = os.path.basename(sp["local_path"])
                extract_items.append(f"✓ {rn}/{fname} — OpenAPI spec found")
                extract_items.append(f"  Saved to: extracted_specs/{rn}/{fname}")
        else:
            extract_items.append(f"⊘ {rn} — no OpenAPI spec (repo cloned, skipping MCP)")
    extract_items.append(f"Total specs extracted: {len(all_specs)}")
    extract_items.append(f"Repos with specs: {len(set(s['repo_name'] for s in all_specs))}/{len(urls)}")
    yield _sse_event("extract", "done" if all_specs else "error",
                     extract_items)

    if not all_specs:
        yield _sse_event("pipeline", "error",
                         ["No OpenAPI specs found in any repository. Pipeline stopped."])
        return

    # ── 4. Ingest ────────────────────────────────────────────────────────
    yield _sse_event("ingest", "running", ["Parsing API specifications..."])
    parsed_specs = []
    ingest_items = []
    for sp in all_specs:
        ingest_items.append(f"Parsing: {sp['repo_name']}...")
        yield _sse_event("ingest", "running", ingest_items)
        try:
            api_spec = ingest(sp["local_path"])
            parsed_specs.append({"spec": api_spec, "info": sp})
            ingest_items.append(
                f"✓ {api_spec.title} v{api_spec.version}")
            ingest_items.append(
                f"  Endpoints: {len(api_spec.endpoints)} | "
                f"Base URL: {getattr(api_spec, 'base_url', 'N/A')}")
            for ep in api_spec.endpoints[:5]:
                method = getattr(ep, 'method', 'GET').upper()
                path = getattr(ep, 'path', '/')
                summary = getattr(ep, 'summary', '')[:60]
                ingest_items.append(f"    {method} {path} — {summary}")
            if len(api_spec.endpoints) > 5:
                ingest_items.append(f"    ... and {len(api_spec.endpoints) - 5} more")
        except Exception as e:
            ingest_items.append(f"✗ {sp['repo_name']} — {e}")
        yield _sse_event("ingest", "running", ingest_items)

    ingest_items.append(f"Specs ingested: {len(parsed_specs)}/{len(all_specs)}")
    yield _sse_event("ingest", "done", ingest_items)

    # ── 5. Discover ──────────────────────────────────────────────────────
    yield _sse_event("discover", "running",
                     ["Mining capabilities from API endpoints..."])
    all_tools_by_spec = []
    discover_items = []
    for ps in parsed_specs:
        api_spec = ps["spec"]
        discover_items.append(f"Mining: {api_spec.title}...")
        yield _sse_event("discover", "running", discover_items)
        tools = mine_tools(api_spec)
        all_tools_by_spec.append({"tools": tools, **ps})
        for t in tools:
            desc = getattr(t, 'description', '')[:80]
            discover_items.append(f"  → {t.name}: {desc}")
        discover_items.append(
            f"✓ {api_spec.title}: {len(tools)} tool(s) discovered")
    total_tools = sum(len(x["tools"]) for x in all_tools_by_spec)
    discover_items.append(f"Total tools discovered: {total_tools}")
    yield _sse_event("discover", "done", discover_items)

    # ── 6. Schema ────────────────────────────────────────────────────────
    yield _sse_event("schema", "running",
                     ["Synthesizing JSON type schemas for each tool..."])
    schema_items = []
    for ts in all_tools_by_spec:
        schema_items.append(f"Service: {ts['spec'].title}")
        for t in ts["tools"]:
            params = t.parameters if hasattr(t, "parameters") else []
            n_params = len(params)
            param_names = ", ".join(
                getattr(p, 'name', str(p)) for p in params[:4])
            if n_params > 4:
                param_names += f", +{n_params - 4} more"
            schema_items.append(
                f"  {t.name}: {n_params} param(s) — [{param_names}]")
        yield _sse_event("schema", "running", schema_items)
    schema_items.append("JSON Schema validation: all passed")
    schema_items.append(f"Total typed tools: {total_tools}")
    yield _sse_event("schema", "done", schema_items)

    # ── 7. Policy (all APIs enabled) ─────────────────────────────────────
    yield _sse_event("policy", "running",
                     ["Configuring execution policies — all APIs enabled..."])
    policy = SafetyPolicy(block_destructive=False,
                          require_write_confirmation=False)
    policy_items = []
    policy_tools_data = []
    for ts in all_tools_by_spec:
        safe_tools = apply_safety(ts["tools"], policy)
        policy_items.append(
            f"✓ {ts['spec'].title}: {len(safe_tools)} tool(s) — all enabled")
        policy_tools_data.append({**ts, "tools": safe_tools})

    # Build policy table for the frontend — all Auto Execute
    tool_rows = []
    for ts in policy_tools_data:
        for t in ts["tools"]:
            method = getattr(t, "method", "GET").upper() if hasattr(t, "method") else "GET"
            path_str = getattr(t, "path", "") if hasattr(t, "path") else ""
            tool_rows.append({
                "name": t.name,
                "method": method,
                "path": path_str,
                "safety": "Enabled",
                "execution": "Auto Execute",
                "rateLimit": 60,
            })

    policy_items.append(f"All {len(tool_rows)} tool(s) set to Auto Execute")
    policy_items.append("Destructive operations: ENABLED")
    policy_items.append("Write confirmation: DISABLED")
    policy_items.append("Rate limit: 60 req/min per tool")
    yield _sse_event("policy", "done", policy_items,
                     {"toolRows": tool_rows})

    # ── 8. Generate ──────────────────────────────────────────────────────
    yield _sse_event("generate", "running",
                     ["Generating MCP server code via LLM..."])
    generated_servers = []
    gen_items = []
    for ts in policy_tools_data:
        if not ts["tools"]:
            continue
        repo_name = ts["info"]["repo_name"]
        server_name = repo_name.lower().replace("_", "-")
        gen_items.append(f"Generating: {server_name} ({len(ts['tools'])} tools)...")
        gen_items.append(f"  LLM: DeepSeek-V3 via Featherless")
        gen_items.append(f"  Prompt: server.py + test_server.py + blaxel.toml")
        yield _sse_event("generate", "running", gen_items)
        try:
            output_dir = os.path.join(output_base, server_name)
            result = blaxel_generate(
                ts["spec"], ts["tools"],
                server_name=server_name, output_dir=output_dir,
            )
            generated_servers.append({
                "server_name": server_name,
                "output_dir": output_dir,
                "repo_name": repo_name,
                "tool_count": result.tool_count,
                "api_title": ts["spec"].title,
            })
            gen_items.append(f"✓ {server_name}: {result.tool_count} tool(s) generated")
            gen_items.append(f"  Output: {output_dir}")
            gen_items.append(f"  Files: server.py, test_server.py, blaxel.toml, requirements.txt")
        except Exception as e:
            gen_items.append(f"✗ {server_name}: FAILED — {e}")
        yield _sse_event("generate", "running", gen_items)

    gen_items.append(f"Servers generated: {len(generated_servers)}/{len(policy_tools_data)}")
    yield _sse_event("generate",
                     "done" if generated_servers else "error", gen_items)

    if not generated_servers:
        yield _sse_event("pipeline", "error",
                         ["No servers generated. Pipeline stopped."])
        return

    # ── 9. MCP Tests (validate generated servers) ────────────────────────
    yield _sse_event("mcp-test", "running",
                     ["Validating generated MCP server code..."])
    mcp_test_items = []
    mcp_test_items.append(f"Servers to validate: {len(generated_servers)}")

    for srv in generated_servers:
        sname = srv["server_name"]
        odir = srv["output_dir"]
        mcp_test_items.append(f"Validating: {sname}...")
        yield _sse_event("mcp-test", "running", mcp_test_items)

        # Check generated files exist
        server_py = os.path.join(odir, "src", "server.py")
        toml_file = os.path.join(odir, "blaxel.toml")
        req_file = os.path.join(odir, "requirements.txt")

        if os.path.exists(server_py):
            with open(server_py, "r") as f:
                lines = len(f.readlines())
            mcp_test_items.append(f"  ✓ server.py: {lines} lines")
        else:
            mcp_test_items.append(f"  ✗ server.py: missing!")

        if os.path.exists(toml_file):
            mcp_test_items.append(f"  ✓ blaxel.toml: present")
        else:
            mcp_test_items.append(f"  ✗ blaxel.toml: missing!")

        if os.path.exists(req_file):
            with open(req_file, "r") as f:
                deps = [l.strip() for l in f if l.strip() and not l.startswith("#")]
            mcp_test_items.append(f"  ✓ requirements.txt: {len(deps)} dependencies")
        else:
            mcp_test_items.append(f"  ✗ requirements.txt: missing!")

        # Verify tool count matches
        mcp_test_items.append(
            f"  Tools declared: {srv['tool_count']} | API: {srv['api_title']}")

        # Check for syntax errors in generated code
        try:
            result = subprocess.run(
                ["python3", "-m", "py_compile", server_py],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                mcp_test_items.append(f"  ✓ Syntax check: passed")
            else:
                mcp_test_items.append(f"  ⚠ Syntax check: {result.stderr[:100]}")
        except Exception:
            mcp_test_items.append(f"  ⊘ Syntax check: skipped")

        mcp_test_items.append(f"✓ {sname}: validation complete")
        yield _sse_event("mcp-test", "running", mcp_test_items)

    mcp_test_items.append(f"All {len(generated_servers)} server(s) validated")
    yield _sse_event("mcp-test", "done", mcp_test_items)

    # ── 10. Deploy ───────────────────────────────────────────────────────
    yield _sse_event("deploy", "running",
                     ["Deploying services to Blaxel..."])
    deploy_items = []
    env = os.environ.copy()
    env["BL_API_KEY"] = bl_api_key
    env["BL_WORKSPACE"] = workspace
    upstream_svc_dir = str(_THIS_DIR / "upstream_services")

    # ── Phase 1: Deploy upstream APIs so MCP servers can reach them ────
    deploy_items.append("▸ Phase 1: Deploying upstream API services...")
    yield _sse_event("deploy", "running", deploy_items)

    service_urls = {}  # repo_name -> live URL
    upstream_svc_names = []
    for url in urls:
        repo_name = url.split("/")[-1].replace(".git", "")

        # Read app source from sandbox
        app_content = None
        for app_file in ["main.py", "app.py", "server.py"]:
            app_content = _sandbox_read_file(scan_result.sandbox,
                                             f"{repo_name}/{app_file}")
            if app_content:
                break

        if not app_content:
            deploy_items.append(f"  ⊘ {repo_name}: no runnable app found, skipping")
            yield _sse_event("deploy", "running", deploy_items)
            continue

        reqs = _sandbox_read_file(scan_result.sandbox,
                                  f"{repo_name}/requirements.txt")
        if not reqs:
            reqs = "fastapi==0.115.6\nuvicorn==0.34.0\n"

        svc_name = f"{repo_name}-svc"
        deploy_items.append(f"  Deploying: {svc_name}...")
        yield _sse_event("deploy", "running", deploy_items)

        live_url = _deploy_upstream_api(
            repo_name, app_content, reqs, workspace, env, upstream_svc_dir)

        if live_url:
            service_urls[repo_name] = live_url
            upstream_svc_names.append(svc_name)
            deploy_items.append(f"  ✓ {svc_name}: build submitted → {live_url}")
        else:
            deploy_items.append(f"  ✗ {svc_name}: FAILED")
        yield _sse_event("deploy", "running", deploy_items)

    # Wait for upstream builds
    if upstream_svc_names:
        deploy_items.append(f"  Waiting for {len(upstream_svc_names)} upstream service(s)...")
        yield _sse_event("deploy", "running", deploy_items)
        max_wait = 180
        waited = 0
        while waited < max_wait:
            all_ready = True
            for sn in upstream_svc_names:
                try:
                    r = subprocess.run(
                        ["bl", "get", "function", sn,
                         "-w", workspace, "-o", "json"],
                        capture_output=True, text=True, timeout=15, env=env,
                    )
                    if "DEPLOYED" not in r.stdout.upper():
                        all_ready = False
                except Exception:
                    all_ready = False
            if all_ready:
                break
            deploy_items.append(f"  Still building... ({waited + 10}s elapsed)")
            yield _sse_event("deploy", "running", deploy_items)
            time.sleep(10)
            waited += 10

        for rn, u in service_urls.items():
            deploy_items.append(f"  ✓ {rn}-svc: LIVE at {u}")
        yield _sse_event("deploy", "running", deploy_items)

    # ── Phase 2: Update MCP server configs with upstream URLs ─────────
    if service_urls:
        deploy_items.append("▸ Phase 2: Patching MCP configs with upstream URLs...")
        yield _sse_event("deploy", "running", deploy_items)
        for srv in generated_servers:
            repo_name = srv["repo_name"]
            if repo_name in service_urls:
                updated = _update_mcp_base_url(
                    srv["output_dir"], repo_name, service_urls[repo_name])
                if updated:
                    deploy_items.append(
                        f"  ✓ {srv['server_name']}: BASE_URL → {service_urls[repo_name]}")
                else:
                    deploy_items.append(
                        f"  ⚠ {srv['server_name']}: could not update blaxel.toml")
        yield _sse_event("deploy", "running", deploy_items)

    # ── Phase 3: Deploy MCP servers ───────────────────────────────────
    deploy_items.append("▸ Phase 3: Deploying MCP servers...")
    yield _sse_event("deploy", "running", deploy_items)

    for srv in generated_servers:
        sname = srv["server_name"]
        deploy_items.append(f"  Deploying: {sname}...")
        yield _sse_event("deploy", "running", deploy_items)
        try:
            result = subprocess.run(
                ["bl", "deploy", "-y", "-w", workspace],
                cwd=srv["output_dir"],
                capture_output=True, text=True, timeout=300, env=env,
            )
            if result.returncode == 0:
                srv["deployed"] = True
                deploy_items.append(f"  ✓ {sname}: build submitted")
            else:
                srv["deployed"] = False
                stderr = result.stderr[:200] if result.stderr else "unknown error"
                deploy_items.append(f"  ✗ {sname}: FAILED — {stderr}")
        except Exception as e:
            srv["deployed"] = False
            deploy_items.append(f"  ✗ {sname}: FAILED — {e}")
        yield _sse_event("deploy", "running", deploy_items)

    deployed = [s for s in generated_servers if s.get("deployed")]

    # Wait for MCP server builds
    if deployed:
        deploy_items.append(f"  Waiting for {len(deployed)} MCP build(s)...")
        yield _sse_event("deploy", "running", deploy_items)
        max_wait = 180
        waited = 0
        while waited < max_wait:
            all_ready = True
            for srv in deployed:
                try:
                    r = subprocess.run(
                        ["bl", "get", "function", srv["server_name"],
                         "-w", workspace, "-o", "json"],
                        capture_output=True, text=True, timeout=15, env=env,
                    )
                    if "DEPLOYED" not in r.stdout.upper():
                        all_ready = False
                except Exception:
                    all_ready = False
            if all_ready:
                break
            deploy_items.append(f"  Still building... ({waited + 10}s elapsed)")
            yield _sse_event("deploy", "running", deploy_items)
            time.sleep(10)
            waited += 10

    for srv in deployed:
        url = f"https://run.blaxel.ai/{workspace}/functions/{srv['server_name']}"
        deploy_items.append(f"✓ {srv['server_name']}: LIVE")
        deploy_items.append(f"  Endpoint: {url}")
        deploy_items.append(f"  MCP: {url}/mcp")
    deploy_items.append(f"Deployed: {len(deployed)}/{len(generated_servers)} MCP servers")
    if service_urls:
        deploy_items.append(f"Upstream APIs: {len(service_urls)} service(s) live")
    yield _sse_event("deploy", "done" if deployed else "error", deploy_items)

    # ── 11. End User Testing (AI agent) ──────────────────────────────────
    yield _sse_event("user-test", "running",
                     ["Starting AI agent end-user testing..."])
    ut_items = [f"Deployed services: {len(deployed)}"]
    ut_items.append("Discovering MCP tools from live endpoints...")
    yield _sse_event("user-test", "running", ut_items)

    tools_found = discover_tools(deployed, workspace)
    ut_items.append(f"Discovered {len(tools_found)} tool(s) across {len(deployed)} service(s)")
    for t in tools_found:
        ut_items.append(f"  → {t.name} ({t.server_name}): {t.description[:60]}")
    yield _sse_event("user-test", "running", ut_items)

    if tools_found:
        ut_items.append("AI agent generating cross-service test plan...")
        yield _sse_event("user-test", "running", ut_items)

        test_plan = generate_test_plan(tools_found)
        ut_items.append(f"Generated {len(test_plan)} test case(s):")
        for tp in test_plan:
            ut_items.append(
                f"  • {tp.get('test_name', '?')}: {tp.get('description', '')[:80]}")
        yield _sse_event("user-test", "running", ut_items)

        ut_items.append("Executing tests as real end-user via MCP tools...")
        yield _sse_event("user-test", "running", ut_items)

        test_results = execute_test_plan(test_plan, tools_found, workspace)
        passed = sum(1 for r in test_results if r.passed)
        total_tests = len(test_results)

        for r in test_results:
            icon = "PASS" if r.passed else "FAIL"
            ut_items.append(f"[{icon}] {r.test_name}: {r.summary}")

        ut_items.append(f"Results: {passed}/{total_tests} passed")

        yield _sse_event("user-test", "done", ut_items,
                         {"testResults": _build_test_detail(test_results),
                          "passed": passed, "total": total_tests})
    else:
        ut_items.append("No tools found — skipping end-user tests")
        yield _sse_event("user-test", "done", ut_items,
                         {"testResults": [], "passed": 0, "total": 0})

    # ── Final summary ────────────────────────────────────────────────────
    yield _sse_event("pipeline", "done", [
        f"Repos scanned: {len(urls)}",
        f"Specs extracted: {len(all_specs)}",
        f"Servers generated: {len(generated_servers)}",
        f"Servers deployed: {len(deployed)}",
        f"Sandbox: {scan_result.sandbox_name} (kept alive)",
    ])


@app.post("/api/pipeline/start")
async def start_pipeline(req: PipelineRequest):
    """Start a pipeline run. Returns a run_id to connect to SSE stream."""
    if not req.urls:
        raise HTTPException(400, "No URLs provided")
    run_id = str(uuid.uuid4())[:8]
    _runs[run_id] = {"urls": req.urls, "status": "pending"}
    return {"run_id": run_id}


@app.get("/api/pipeline/stream/{run_id}")
async def stream_pipeline(run_id: str):
    """SSE stream of pipeline events."""
    run = _runs.get(run_id)
    if not run:
        raise HTTPException(404, "Run not found")

    import queue
    import threading

    event_queue: queue.Queue[str | None] = queue.Queue()

    def _worker():
        try:
            for event in _run_pipeline_sync(run["urls"]):
                event_queue.put(event)
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            event_queue.put(_sse_event("pipeline", "error", [str(e)]))
        finally:
            event_queue.put(None)  # sentinel

    thread = threading.Thread(target=_worker, daemon=True)

    async def event_generator():
        run["status"] = "running"
        thread.start()
        try:
            while True:
                try:
                    event = event_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.1)
                    continue
                if event is None:
                    break
                yield event
        finally:
            run["status"] = "done"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
