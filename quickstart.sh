#!/bin/bash
# quickstart.sh - Setup script for sageFlow development

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Get project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo -e "${CYAN}${BOLD}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}${BOLD}║              SageFlow Development Setup                    ║${NC}"
echo -e "${CYAN}${BOLD}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Step 1: Install Git Hooks
echo -e "${YELLOW}${BOLD}Step 1: Installing Git Hooks${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

HOOKS_DIR="$PROJECT_ROOT/.git/hooks"
TEMPLATE_DIR="$PROJECT_ROOT/hooks"

if [ ! -d "$HOOKS_DIR" ]; then
    echo -e "${RED}✗ Git repository not initialized${NC}"
    echo -e "${YELLOW}Run: git init${NC}"
    exit 1
fi

# Install hooks
if [ -d "$TEMPLATE_DIR" ]; then
    for hook in pre-commit pre-push post-commit; do
        if [ -f "$TEMPLATE_DIR/$hook" ]; then
            cp "$TEMPLATE_DIR/$hook" "$HOOKS_DIR/$hook"
            chmod +x "$HOOKS_DIR/$hook"
            echo -e "${GREEN}✓ Installed $hook hook${NC}"
        fi
    done
else
    echo -e "${YELLOW}⚠ No hooks directory found, skipping...${NC}"
fi

echo ""

# Step 2: Install in development mode
echo -e "${YELLOW}${BOLD}Step 2: Installing isage-flow in development mode${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}Note: This will build the C++ extension (requires cmake, build-essential)${NC}"
echo ""

pip install -e .

echo ""
echo -e "${GREEN}${BOLD}✓ Setup complete!${NC}"
echo ""
echo -e "${CYAN}Next steps:${NC}"
echo -e "  ${GREEN}1.${NC} Make changes to the code"
echo -e "  ${GREEN}2.${NC} Run tests: ${YELLOW}pytest tests/${NC} or ${YELLOW}ctest --test-dir build${NC}"
echo -e "  ${GREEN}3.${NC} Commit changes: ${YELLOW}git add . && git commit -m 'your message'${NC}"
echo -e "  ${GREEN}4.${NC} Push to GitHub: ${YELLOW}git push${NC} (pre-push hook will check version)"
echo ""
