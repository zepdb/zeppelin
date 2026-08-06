#!/usr/bin/env python3
"""Refresh the embedded cloud catalog's instance specs and prices.

Scope: this script refreshes the ``[[instance]]`` arrays (and ``[meta]``)
of ``src/sizing/catalog_data/{aws,gcp,azure}.toml``. Block-storage tiers,
object-store price sheets, and the RTT matrix are hand-curated — they are
a handful of slowly-moving numbers and the script deliberately preserves
them verbatim.

Sources (no credentials needed unless noted):
  aws    AWS's region-scoped Bulk Price List CSV, streamed and filtered to
         the allowlist, plus Vantage open data for hardware/network fields
         the price list lacks. ``--source-url`` overrides Vantage only.
  gcp    Vantage open data for GCP. The Cloud Billing Catalog API is more
         authoritative but needs ``--gcp-api-key``; when given it is used
         to cross-check prices and drift is reported.
  azure  The public, unauthenticated Azure Retail Prices API, filtered per
         SKU family and region.

Modes:
  default      rewrite the target files in place, stamping today's date.
  --check      regenerate to memory keeping the existing snapshot_date and
               compare semantically (parsed values, 1e-9 tolerance on
               prices); exit 1 and print field-level drift when they
               differ. CI-friendly.
  --fixtures D read source payloads from files in D instead of the
               network: ``aws_vantage.json``, ``aws_ec2_REGION.csv``,
               ``gcp_vantage.json``, and ``azure_retail_*.json``. Used by
               the offline determinism test against
               ``scripts/fixtures/catalog/expected/``.

The emitted TOML is canonical (sorted, no hand comments); the checked-in
files may carry extra comments, which the semantic comparison ignores.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import re
import sys
import tomllib
import urllib.parse
import urllib.request
from pathlib import Path

# The curated scope: exactly the SKUs the embedded catalog tracks.
AWS_INSTANCES = [
    *[
        f"{family}.{size}"
        for family in [
            "m7i", "m7g", "c7i", "c7g", "r7i", "r7g",
            "i4i", "im4gn", "m6i",
        ]
        for size in ["xlarge", "2xlarge", "4xlarge", "8xlarge"]
    ],
    "i3en.xlarge", "i3en.2xlarge", "i3en.3xlarge", "i3en.6xlarge",
]
AWS_REGIONS = [
    "us-east-1", "us-west-2", "eu-west-1",
    "eu-central-1", "ap-south-1", "ap-southeast-1",
]
GCP_INSTANCES = [
    "n2-standard-4", "n2-standard-8", "n2-standard-16", "n2-standard-32",
    "n2d-standard-4", "n2d-standard-8", "n2d-standard-16", "n2d-standard-32",
    "c3-standard-4", "c3-standard-8", "c3-standard-22", "c3-standard-44",
    "c3d-standard-4", "c3d-standard-8", "c3d-standard-16", "c3d-standard-30",
    "z3-highmem-8-highlssd", "z3-highmem-14-standardlssd",
    "z3-highmem-16-highlssd", "z3-highmem-22-standardlssd",
    "z3-highmem-22-highlssd",
]
GCP_REGIONS = [
    "us-central1", "us-west1", "europe-west1",
    "europe-west4", "asia-south1", "asia-southeast1",
]
AZURE_INSTANCES = [
    "D4s_v5", "D8s_v5", "D16s_v5", "D32s_v5",
    "D4ds_v5", "D8ds_v5", "D16ds_v5", "D32ds_v5",
    "E4s_v5", "E8s_v5", "E16s_v5", "E32s_v5",
    "E4ds_v5", "E8ds_v5",
    "L8s_v3", "L16s_v3", "L32s_v3",
]
AZURE_REGIONS = [
    "eastus", "westus2", "westeurope",
    "germanywestcentral", "centralindia", "southeastasia",
]

VANTAGE_AWS_URL = "https://instances.vantage.sh/instances.json"
VANTAGE_GCP_URL = "https://instances.vantage.sh/gcp/instances.json"
AZURE_RETAIL_URL = "https://prices.azure.com/api/retail/prices"
AWS_BULK_PRICE_URL = (
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/"
    "AmazonEC2/current/{region}/index.csv"
)
GCP_BILLING_CATALOG_URL = (
    "https://cloudbilling.googleapis.com/v1/services/6F81-5844-456A/skus"
)

PRICE_TOLERANCE = 1e-9


def fetch_json(url: str) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": "zeppelin-catalog-refresh"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def parse_aws_region_prices(stream: object, region: str,
                            wanted: list[str]) -> dict[str, float]:
    """Filter one AWS Bulk Price List CSV stream to Linux on-demand rows."""
    found: dict[str, set[float]] = {}
    for _ in range(5):
        if stream.readline() == "":
            raise SystemExit(f"aws: truncated Bulk Price List for {region}")
    for row in csv.DictReader(stream):
        name = row.get("Instance Type")
        if name not in wanted:
            continue
        if row.get("Product Family") != "Compute Instance":
            continue
        if row.get("Operating System") != "Linux":
            continue
        if row.get("Tenancy") != "Shared":
            continue
        if row.get("Pre Installed S/W") not in ("NA", ""):
            continue
        if row.get("TermType") != "OnDemand" or row.get("Unit") != "Hrs":
            continue
        if row.get("CapacityStatus") not in ("Used", ""):
            continue
        price = parse_float(row.get("PricePerUnit"), f"{name}/{region}")
        if price > 0.0:
            found.setdefault(name, set()).add(price)

    prices = {}
    for name in wanted:
        candidates = found.get(name, set())
        if len(candidates) != 1:
            raise SystemExit(
                f"aws: expected one Linux on-demand price for {name}/{region}, "
                f"found {sorted(candidates)}")
        prices[name] = candidates.pop()
    return prices


def fetch_aws_region_prices(region: str, wanted: list[str]) -> dict[str, float]:
    """Stream one region-scoped EC2 Bulk Price List from AWS."""
    url = AWS_BULK_PRICE_URL.format(region=region)
    request = urllib.request.Request(
        url, headers={"User-Agent": "zeppelin-catalog-refresh"})
    with urllib.request.urlopen(request, timeout=120) as response:
        stream = io.TextIOWrapper(response, encoding="utf-8-sig", newline="")
        return parse_aws_region_prices(stream, region, wanted)


def fixture_aws_region_prices(fixtures: Path, region: str,
                              wanted: list[str]) -> dict[str, float]:
    path = fixtures / f"aws_ec2_{region}.csv"
    if not path.exists():
        raise SystemExit(f"fixture {path} is missing")
    with path.open(newline="") as stream:
        return parse_aws_region_prices(stream, region, wanted)


def apply_aws_bulk_prices(instances: list[dict], fixtures: Path | None) -> None:
    """Replace machine-catalog prices with official regional AWS prices."""
    by_name = {row["name"]: row for row in instances}
    refreshed = {name: {} for name in by_name}
    regions = sorted({region for row in instances for region in row["price_hr"]})
    for region in regions:
        wanted = [
            name for name, row in by_name.items()
            if region in row["price_hr"]
        ]
        prices = (fixture_aws_region_prices(fixtures, region, wanted)
                  if fixtures is not None
                  else fetch_aws_region_prices(region, wanted))
        for name, price in prices.items():
            vantage = by_name[name]["price_hr"].get(region)
            if vantage is not None and abs(float(vantage) - price) > PRICE_TOLERANCE:
                print(
                    f"WARNING aws: {name}/{region} Vantage {vantage} -> "
                    f"Bulk Price List {price}",
                    file=sys.stderr,
                )
            refreshed[name][region] = price
    for name, prices in refreshed.items():
        by_name[name]["price_hr"] = prices


def load_fixture(fixtures: Path, name: str) -> object:
    path = fixtures / name
    if not path.exists():
        raise SystemExit(f"fixture {path} is missing")
    return json.loads(path.read_text())


def fetch_gcp_catalog(api_key: str) -> list[object]:
    """Fetch every current Compute Engine SKU from Cloud Billing."""
    pages = []
    page_token: str | None = None
    while True:
        query = {
            "key": api_key,
            "currencyCode": "USD",
            "pageSize": "5000",
        }
        if page_token:
            query["pageToken"] = page_token
        page = fetch_json(
            f"{GCP_BILLING_CATALOG_URL}?{urllib.parse.urlencode(query)}")
        if not isinstance(page, dict) or not isinstance(page.get("skus"), list):
            raise SystemExit("gcp: Billing Catalog response contains no skus array")
        pages.append(page)
        page_token = page.get("nextPageToken")
        if not page_token:
            return pages


def money_value(value: object, context: str) -> float:
    """Decode the Google APIs Money JSON representation."""
    if not isinstance(value, dict):
        raise SystemExit(f"gcp: {context} has no unitPrice object")
    units = parse_float(value.get("units", 0), f"{context} units")
    nanos = parse_float(value.get("nanos", 0), f"{context} nanos")
    return units + nanos / 1_000_000_000.0


def gcp_sku_rate(sku: dict) -> float:
    """Return the current first-tier unit rate for one Billing SKU."""
    pricing = sku.get("pricingInfo")
    if not isinstance(pricing, list) or not pricing:
        raise SystemExit(f"gcp: SKU {sku.get('description')!r} has no pricingInfo")
    expression = pricing[-1].get("pricingExpression", {})
    rates = expression.get("tieredRates", [])
    if not rates:
        raise SystemExit(f"gcp: SKU {sku.get('description')!r} has no tieredRates")
    return money_value(rates[0].get("unitPrice"), str(sku.get("description")))


def apply_gcp_catalog_prices(instances: list[dict], pages: list[object]) -> None:
    """Cross-check and replace composed VM rates with Billing Catalog rates.

    Standard N2/N2D/C3/C3D VMs are billed as one core SKU plus one RAM SKU,
    so their machine rate is reconstructed exactly. Z3's bundled Titanium
    SSD is a separate meter; those composite prices remain sourced from the
    Vantage machine catalog and are called out explicitly.
    """
    skus = []
    for page in pages:
        skus.extend(page.get("skus", []))
    family_labels = {
        "n2": "N2 Instance",
        "n2d": "N2D Instance",
        "c3": "C3 Instance",
        "c3d": "C3D Instance",
    }

    def find_rate(label: str, resource: str, region: str) -> float:
        prefix = f"{label} {resource} running in "
        matches = []
        for sku in skus:
            if not isinstance(sku, dict):
                continue
            description = str(sku.get("description", ""))
            category = sku.get("category", {})
            if not description.startswith(prefix):
                continue
            if category.get("usageType") != "OnDemand":
                continue
            if region not in sku.get("serviceRegions", []):
                continue
            matches.append(sku)
        if len(matches) != 1:
            raise SystemExit(
                f"gcp: expected one {label} {resource} SKU for {region}, "
                f"found {len(matches)}")
        return gcp_sku_rate(matches[0])

    checked = 0
    for row in instances:
        label = family_labels.get(row["family"])
        if label is None:
            continue
        catalog_prices = {}
        for region, vantage_price in row["price_hr"].items():
            core = find_rate(label, "Core", region)
            ram = find_rate(label, "Ram", region)
            catalog_price = row["vcpus"] * core + row["mem_gb"] * ram
            tolerance = max(0.0002, float(vantage_price) * 0.002)
            if abs(catalog_price - float(vantage_price)) > tolerance:
                raise SystemExit(
                    f"gcp: {row['name']}/{region} Vantage price "
                    f"{vantage_price:.6f} disagrees with Billing Catalog "
                    f"composition {catalog_price:.6f}")
            catalog_prices[region] = catalog_price
            checked += 1
        row["price_hr"] = catalog_prices
    if checked == 0:
        raise SystemExit("gcp: Billing Catalog cross-check covered no VM prices")
    print(
        f"gcp: Billing Catalog cross-checked {checked} N2/N2D/C3/C3D prices; "
        "Z3 retains its bundled machine-catalog price",
        file=sys.stderr,
    )


def parse_float(value: object, context: str) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        raise SystemExit(f"unparseable number {value!r} in {context}") from None


def vantage_instances(payload: object, wanted: list[str], regions: list[str],
                      cloud: str) -> list[dict]:
    """Extract our SKU subset from a Vantage-style instances dump.

    Fields consumed per entry: ``instance_type``, ``vCPU``, ``memory``,
    ``arch`` (or ``physical_processor`` fallback), ``storage`` (NVMe GB),
    ``network_baseline_gbps``/``network_burst_gbps`` (or
    ``network_performance`` prose), and
    ``pricing.<region>.linux.ondemand``.
    """
    if not isinstance(payload, list):
        raise SystemExit(f"{cloud}: source payload is not a list")
    by_name = {}
    for entry in payload:
        name = entry.get("instance_type")
        if name in wanted:
            by_name[name] = entry
    missing = sorted(set(wanted) - set(by_name))
    if missing:
        raise SystemExit(
            f"{cloud}: source lacks {len(missing)} SKUs: {', '.join(missing)}")
    instances = []
    for name in sorted(by_name):
        entry = by_name[name]
        arch = entry.get("arch")
        if arch not in ("x86_64", "arm64"):
            processor = str(entry.get("physical_processor", ""))
            arch = "arm64" if re.search(r"graviton|ampere|arm", processor, re.I) else "x86_64"
        baseline = entry.get("network_baseline_gbps")
        burst = entry.get("network_burst_gbps")
        if baseline is None or burst is None:
            baseline, burst = parse_network_performance(
                str(entry.get("network_performance", "")), name)
        prices = {}
        pricing = entry.get("pricing", {})
        for region in regions:
            ondemand = (pricing.get(region, {}).get("linux", {}) or {}).get("ondemand")
            if ondemand is not None:
                prices[region] = parse_float(ondemand, f"{name}/{region}")
        if not prices:
            raise SystemExit(
                f"{cloud}: {name} has no prices in the requested regions")
        instances.append({
            "name": name,
            "family": name.split(".")[0] if "." in name else name.split("-", 1)[0],
            "arch": arch,
            "vcpus": int(parse_float(entry.get("vCPU"), f"{name} vCPU")),
            "mem_gb": parse_float(entry.get("memory"), f"{name} memory"),
            "nvme_gb": int(parse_float(entry.get("storage") or 0, f"{name} storage")),
            "network_baseline_gbps": parse_float(baseline, f"{name} baseline"),
            "network_burst_gbps": parse_float(burst, f"{name} burst"),
            "price_hr": prices,
        })
    return instances


def require_region_coverage(instances: list[dict], regions: list[str],
                            cloud: str) -> None:
    """Reject a live source that would silently drop a target region."""
    required = set(regions)
    for row in instances:
        missing = sorted(required - set(row["price_hr"]))
        if missing:
            raise SystemExit(
                f"{cloud}: {row['name']} lacks prices for " + ", ".join(missing))


def parse_network_performance(prose: str, name: str) -> tuple[float, float]:
    """Turn AWS 'Up to 12.5 Gigabit' / '25 Gigabit' prose into numbers.

    'Up to X' means burst = X with an undisclosed lower baseline; absent a
    baseline column we conservatively use X/4, the documented ratio for
    most burstable Nitro sizes. A bare figure is a sustained allocation.
    """
    match = re.search(r"([\d.]+)\s*Gigabit", prose)
    if not match:
        raise SystemExit(f"{name}: cannot parse network performance {prose!r}")
    figure = float(match.group(1))
    if re.search(r"up to", prose, re.I):
        return figure / 4.0, figure
    return figure, figure


def azure_retail_instances(payloads: list[object], wanted: list[str],
                           regions: list[str]) -> dict[str, dict[str, float]]:
    """Collect armSkuName -> region -> hourly price from retail API pages."""
    candidates: dict[str, dict[str, set[float]]] = {}
    for payload in payloads:
        items = payload.get("Items", []) if isinstance(payload, dict) else []
        for item in items:
            sku = str(item.get("armSkuName", "")).removeprefix("Standard_")
            region = item.get("armRegionName")
            if sku not in wanted or region not in regions:
                continue
            if item.get("type") != "Consumption":
                continue
            meter = str(item.get("meterName", ""))
            product = str(item.get("productName", ""))
            if "Spot" in meter or "Low Priority" in meter or "Windows" in product:
                continue
            service = str(item.get("serviceName", ""))
            if service and service != "Virtual Machines":
                continue
            if product and not product.startswith("Virtual Machines"):
                continue
            candidates.setdefault(sku, {}).setdefault(region, set()).add(
                parse_float(item.get("retailPrice"), f"{sku}/{region}"))
    prices: dict[str, dict[str, float]] = {}
    for sku, by_region in candidates.items():
        for region, values in by_region.items():
            if len(values) != 1:
                raise SystemExit(
                    f"azure: ambiguous Consumption prices for {sku}/{region}: "
                    f"{sorted(values)}")
            prices.setdefault(sku, {})[region] = values.pop()
    return prices


def azure_fetch_pages(fixtures: Path | None) -> list[object]:
    if fixtures is not None:
        return [load_fixture(fixtures, path.name)
                for path in sorted(fixtures.glob("azure_retail_*.json"))]
    pages = []
    for family_filter in ("Dsv5", "Ddsv5", "Esv5", "Edsv5", "Lsv3"):
        query = urllib.parse.urlencode({
            "$filter": "serviceName eq 'Virtual Machines' and "
                       f"contains(productName, '{family_filter}') and "
                       "priceType eq 'Consumption'",
        })
        url = f"{AZURE_RETAIL_URL}?{query}"
        while url:
            page = fetch_json(url)
            pages.append(page)
            url = page.get("NextPageLink") if isinstance(page, dict) else None
    return pages


def azure_instances(pages: list[object], existing: list[dict]) -> list[dict]:
    """Merge refreshed retail prices onto existing spec rows.

    The retail API carries prices, not specs; vCPU/memory/NVMe/network
    columns are stable per SKU and are preserved from the checked-in file.
    A SKU missing from the checked-in file is reported, not invented.
    """
    prices = azure_retail_instances(pages, AZURE_INSTANCES, AZURE_REGIONS)
    by_name = {row["name"]: dict(row) for row in existing}
    unknown = sorted(set(prices) - set(by_name))
    if unknown:
        raise SystemExit(
            "azure: retail API priced SKUs without checked-in specs: "
            + ", ".join(unknown))
    refreshed = []
    for name in sorted(by_name):
        row = by_name[name]
        if name not in prices:
            raise SystemExit(f"azure: no refreshed price for {name}")
        required_regions = set(row.get("price_hr", {}))
        missing_regions = sorted(required_regions - set(prices[name]))
        if missing_regions:
            raise SystemExit(
                f"azure: {name} is missing prices for "
                + ", ".join(missing_regions))
        row["price_hr"] = dict(sorted(prices[name].items()))
        refreshed.append(row)
    return refreshed


def toml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def format_float(value: float) -> str:
    text = f"{value:.6f}".rstrip("0")
    return text + "0" if text.endswith(".") else text


def render_cloud_file(cloud: str, snapshot_date: str, sources: list[str],
                      instances: list[dict], block_storage_raw: str) -> str:
    out = [
        f"# {cloud.upper()} instance catalog — generated by scripts/refresh_cloud_catalog.py.",
        "# Block-storage tiers below the marker are hand-curated and preserved.",
        "[meta]",
        f'cloud = "{cloud}"',
        f'snapshot_date = "{snapshot_date}"',
        'currency = "USD"',
        "sources = [" + ", ".join(f'"{toml_escape(s)}"' for s in sources) + "]",
        "",
    ]
    for row in instances:
        out.append("[[instance]]")
        out.append(f'name = "{toml_escape(row["name"])}"')
        out.append(f'family = "{toml_escape(row["family"])}"')
        out.append(f'arch = "{row["arch"]}"')
        out.append(f'vcpus = {row["vcpus"]}')
        out.append(f'mem_gb = {format_float(row["mem_gb"])}')
        out.append(f'nvme_gb = {row["nvme_gb"]}')
        out.append(f'network_baseline_gbps = {format_float(row["network_baseline_gbps"])}')
        out.append(f'network_burst_gbps = {format_float(row["network_burst_gbps"])}')
        pairs = ", ".join(f"{region} = {format_float(price)}"
                          for region, price in sorted(row["price_hr"].items()))
        out.append(f"price_hr = {{ {pairs} }}")
        out.append("")
    out.append(block_storage_raw.rstrip("\n"))
    out.append("")
    return "\n".join(out)


BLOCK_STORAGE_MARKER = "[[block_storage]]"


def split_block_storage(existing_text: str, path: Path) -> str:
    index = existing_text.find(BLOCK_STORAGE_MARKER)
    if index < 0:
        raise SystemExit(f"{path} has no {BLOCK_STORAGE_MARKER} section to preserve")
    return existing_text[index:]


def semantic_instances(text: str, path: str) -> tuple[str, list[dict]]:
    parsed = tomllib.loads(text)
    if "instance" not in parsed:
        raise SystemExit(f"{path} contains no [[instance]] entries")
    return parsed["meta"]["snapshot_date"], parsed["instance"]


def diff_instances(old: list[dict], new: list[dict]) -> list[str]:
    drift = []
    old_map = {row["name"]: row for row in old}
    new_map = {row["name"]: row for row in new}
    for name in sorted(set(old_map) | set(new_map)):
        if name not in old_map:
            drift.append(f"+ {name} (new SKU)")
            continue
        if name not in new_map:
            drift.append(f"- {name} (dropped SKU)")
            continue
        for key in sorted(set(old_map[name]) | set(new_map[name])):
            old_value = old_map[name].get(key)
            new_value = new_map[name].get(key)
            if key == "price_hr":
                for region in sorted(set(old_value or {}) | set(new_value or {})):
                    a = (old_value or {}).get(region)
                    b = (new_value or {}).get(region)
                    if a is None or b is None or abs(a - b) > PRICE_TOLERANCE:
                        drift.append(f"~ {name}.price_hr.{region}: {a} -> {b}")
            elif isinstance(old_value, float) or isinstance(new_value, float):
                if old_value is None or new_value is None or \
                        abs(float(old_value) - float(new_value)) > PRICE_TOLERANCE:
                    drift.append(f"~ {name}.{key}: {old_value} -> {new_value}")
            elif old_value != new_value:
                drift.append(f"~ {name}.{key}: {old_value} -> {new_value}")
    return drift


def refresh_cloud(cloud: str, out_dir: Path, fixtures: Path | None,
                  source_url: str | None, check: bool,
                  snapshot_date: str | None,
                  gcp_api_key: str | None) -> bool:
    """Returns True when --check found drift."""
    target = out_dir / f"{cloud}.toml"
    comparison_target = target
    if fixtures is not None and check:
        comparison_target = fixtures / "expected" / f"{cloud}.toml"
        if not comparison_target.exists():
            raise SystemExit(
                f"fixture expectation {comparison_target} is missing")
    existing_text = comparison_target.read_text()
    existing_date, existing_instances = semantic_instances(
        existing_text, str(comparison_target))
    block_storage_raw = split_block_storage(existing_text, comparison_target)

    fixture_names = [row["name"] for row in existing_instances]

    if cloud == "aws":
        payload = (load_fixture(fixtures, "aws_vantage.json") if fixtures
                   else fetch_json(source_url or VANTAGE_AWS_URL))
        wanted = fixture_names if fixtures else AWS_INSTANCES
        instances = vantage_instances(payload, wanted, AWS_REGIONS, cloud)
        if fixtures is None:
            require_region_coverage(instances, AWS_REGIONS, cloud)
        sources = [source_url or VANTAGE_AWS_URL]
        apply_aws_bulk_prices(instances, fixtures)
        if fixtures is None:
            sources.append(AWS_BULK_PRICE_URL.format(region="{region}"))
    elif cloud == "gcp":
        payload = (load_fixture(fixtures, "gcp_vantage.json") if fixtures
                   else fetch_json(source_url or VANTAGE_GCP_URL))
        wanted = fixture_names if fixtures else GCP_INSTANCES
        instances = vantage_instances(payload, wanted, GCP_REGIONS, cloud)
        if fixtures is None:
            require_region_coverage(instances, GCP_REGIONS, cloud)
        sources = [source_url or VANTAGE_GCP_URL]
        if gcp_api_key is not None:
            if fixtures is not None:
                raise SystemExit(
                    "--gcp-api-key cannot be combined with offline fixtures")
            apply_gcp_catalog_prices(instances, fetch_gcp_catalog(gcp_api_key))
            sources.append(GCP_BILLING_CATALOG_URL)
    elif cloud == "azure":
        pages = azure_fetch_pages(fixtures)
        instances = azure_instances(pages, existing_instances)
        sources = [AZURE_RETAIL_URL]
    else:
        raise SystemExit(f"unknown cloud {cloud}")

    stamped = snapshot_date or (existing_date if check else dt.date.today().isoformat())
    rendered = render_cloud_file(cloud, stamped, sources, instances,
                                 block_storage_raw)
    if check:
        drift = diff_instances(existing_instances,
                               semantic_instances(rendered, "generated")[1])
        if drift:
            print(f"{cloud}: {len(drift)} drifted entries:")
            for line in drift:
                print(f"  {line}")
            return True
        print(f"{cloud}: no drift")
        return False
    target.write_text(rendered)
    print(f"{cloud}: wrote {target} ({len(instances)} instances, {stamped})")
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cloud", choices=["aws", "gcp", "azure", "all"], default="all")
    parser.add_argument("--out", type=Path,
                        default=Path(__file__).resolve().parent.parent
                        / "src/sizing/catalog_data")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--fixtures", type=Path, default=None)
    parser.add_argument("--source-url", default=None,
                        help="override the Vantage endpoint (aws/gcp only)")
    parser.add_argument("--gcp-api-key", default=None,
                        help="cross-check GCP prices via the Billing Catalog API")
    parser.add_argument("--snapshot-date", default=None,
                        help="stamp this date instead of today (fixture determinism)")
    args = parser.parse_args()

    clouds = ["aws", "gcp", "azure"] if args.cloud == "all" else [args.cloud]
    drifted = False
    for cloud in clouds:
        drifted |= refresh_cloud(cloud, args.out, args.fixtures,
                                 args.source_url, args.check, args.snapshot_date,
                                 args.gcp_api_key)
    return 1 if drifted else 0


if __name__ == "__main__":
    sys.exit(main())
