#!/bin/bash
echo "Opening Test Sheet Automator in your browser..."
DIR="$(cd "$(dirname "$0")" && pwd)"
if command -v xdg-open &> /dev/null; then
    xdg-open "$DIR/index.html"
elif command -v open &> /dev/null; then
    open "$DIR/index.html"
else
    echo "Please open index.html in your browser manually."
fi
