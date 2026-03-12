#!/bin/bash
echo "🔍 Finding processes on port 5050..."
PIDS=$(lsof -ti:5050 2>/dev/null)
if [ -z "$PIDS" ]; then
    echo "✅ No process found on port 5050"
else
    echo "🔪 Killing PIDs: $PIDS"
    echo "$PIDS" | xargs kill -9 2>/dev/null
    sleep 0.5
    echo "✅ Done! Port 5050 is now free"
fi
