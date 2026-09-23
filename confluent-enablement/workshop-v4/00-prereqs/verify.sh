#!/usr/bin/env bash
# verify.sh — Pre-workshop environment check for workshop-v4
# Run this before the workshop day: bash verify.sh
# Every line should print ✅. Fix any ❌ before arriving.

set -euo pipefail

PASS=0
FAIL=0

check() {
  local label="$1"
  local cmd="$2"
  local min_note="$3"
  if eval "$cmd" &>/dev/null; then
    echo "✅  $label"
    ((PASS++))
  else
    echo "❌  $label  ← $min_note"
    ((FAIL++))
  fi
}

echo ""
echo "── workshop-v4 Pre-flight Check ──────────────────────────────────────"
echo ""

# CLI tools
check "terraform 1.0+"      "terraform version | grep -E 'Terraform v[1-9]'"       "brew install terraform"
check "python3 3.9+"        "python3 --version | grep -E 'Python 3\.(9|1[0-9])'"   "brew install python"
check "node 22+"            "node --version | grep -E 'v2[2-9]'"                   "nvm install 22 && nvm use 22"
check "npx available"       "npx --version"                                         "comes with node — check node install"
check "git available"       "git --version"                                         "xcode-select --install"
check "orchestrate CLI"     "orchestrate --version"                                 "pip install ibm-watsonx-orchestrate"

echo ""
echo "── Python dependencies ───────────────────────────────────────────────"

# Check venv + confluent_kafka
if [ -f "02-producer/.venv/bin/python" ]; then
  check "confluent_kafka installed" \
    "02-producer/.venv/bin/python -c 'import confluent_kafka'" \
    "cd 02-producer && pip install -r requirements.txt"
  check "python-dotenv installed" \
    "02-producer/.venv/bin/python -c 'import dotenv'" \
    "cd 02-producer && pip install -r requirements.txt"
else
  echo "⚠️   02-producer/.venv not found — run:"
  echo "     cd 02-producer && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  ((FAIL++))
fi

echo ""
echo "── Terraform provider cache ──────────────────────────────────────────"
if [ -d "01-terraform/.terraform/providers" ]; then
  check "Confluent provider cached" \
    "ls 01-terraform/.terraform/providers/registry.terraform.io/confluentinc/confluent" \
    "cd 01-terraform && terraform init"
else
  echo "⚠️   Terraform providers not cached — run:  cd 01-terraform && terraform init"
  ((FAIL++))
fi

echo ""
echo "── Summary ───────────────────────────────────────────────────────────"
echo "   Passed: $PASS"
echo "   Failed: $FAIL"
echo ""

if [ "$FAIL" -eq 0 ]; then
  echo "🎉 All checks passed — you're ready for the workshop!"
else
  echo "⚠️  Fix the $FAIL item(s) above before the workshop."
fi
echo ""
