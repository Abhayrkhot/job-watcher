#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
read -r -p "Destination email address: " destination
read -r -s -p "Resend API key (starts with re_): " resend_key
printf '\n'
if [[ $destination != *@*.* ]]; then
  echo "That does not look like an email address." >&2
  exit 1
fi
if [[ $resend_key != re_* ]]; then
  echo "That does not look like a Resend API key." >&2
  exit 1
fi
umask 077
printf 'JOB_WATCHER_TO=%q\nJOB_WATCHER_RESEND_API_KEY=%q\n' \
  "$destination" "$resend_key" > .env
mkdir -p state "$HOME/Library/LaunchAgents"
if [[ ! -f state/ats_boards.json ]]; then
  cp bootstrap/ats_boards.json state/ats_boards.json
fi
if [[ ! -f state/feeds.json ]]; then
  cp bootstrap/feeds.json state/feeds.json
fi
/usr/bin/python3 job_watcher.py --initialize
sed -e "s|__PROJECT_DIR__|$SCRIPT_DIR|g" \
  com.personal.job-watcher.plist.template \
  > "$HOME/Library/LaunchAgents/com.personal.job-watcher.plist"
launchctl bootout "gui/$(id -u)/com.personal.job-watcher" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.personal.job-watcher.plist"
echo "Job watcher installed. It checks every five minutes."
