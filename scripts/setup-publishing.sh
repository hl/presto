#!/bin/bash

# Setup script for automated publishing
# This script helps configure the necessary secrets and settings

set -e

echo "🚀 Setting up automated publishing for ex_presto"
echo "================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if we're in the right directory
if [[ ! -f "mix.exs" ]] || ! grep -q "ex_presto" mix.exs; then
    echo -e "${RED}❌ Error: This script must be run from the ex_presto project root${NC}"
    exit 1
fi

echo -e "${BLUE}📋 Prerequisites Checklist${NC}"
echo "================================"

# Check if user is registered with Hex
echo -e "${YELLOW}1. Checking Hex registration...${NC}"
if mix hex.user whoami > /dev/null 2>&1; then
    USER=$(mix hex.user whoami 2>/dev/null | head -1)
    echo -e "${GREEN}✅ Logged in to Hex as: $USER${NC}"
else
    echo -e "${RED}❌ Not logged in to Hex${NC}"
    echo "Please run: mix hex.user register"
    echo "Or if already registered: mix hex.user auth"
    exit 1
fi

# Check if GitHub CLI is available
echo -e "${YELLOW}2. Checking GitHub CLI...${NC}"
if command -v gh &> /dev/null; then
    if gh auth status > /dev/null 2>&1; then
        echo -e "${GREEN}✅ GitHub CLI is authenticated${NC}"
        GH_AVAILABLE=true
    else
        echo -e "${YELLOW}⚠️  GitHub CLI available but not authenticated${NC}"
        echo "Run: gh auth login"
        GH_AVAILABLE=false
    fi
else
    echo -e "${YELLOW}⚠️  GitHub CLI not available${NC}"
    echo "Install from: https://cli.github.com/"
    GH_AVAILABLE=false
fi

echo ""
echo -e "${BLUE}🔑 Generating Hex API Key${NC}"
echo "=========================="

echo "This will generate a new API key for GitHub Actions publishing."
echo "You'll need to enter your Hex credentials."
echo ""

# Generate API key
echo -e "${YELLOW}Generating API key...${NC}"
API_KEY=$(mix hex.user key generate --key-name "github-actions-$(date +%Y%m%d)" --permission api:write 2>/dev/null | tail -1)

if [[ -z "$API_KEY" ]] || [[ ${#API_KEY} -lt 30 ]]; then
    echo -e "${RED}❌ Failed to generate API key${NC}"
    echo "Please run manually: mix hex.user key generate --key-name github-actions --permission api:write"
    exit 1
fi

echo -e "${GREEN}✅ API key generated successfully${NC}"
echo ""

# Set up GitHub secret
echo -e "${BLUE}🔐 Setting up GitHub Secret${NC}"
echo "==========================="

if [[ "$GH_AVAILABLE" == "true" ]]; then
    echo "Setting HEX_API_KEY secret in GitHub repository..."
    
    if echo "$API_KEY" | gh secret set HEX_API_KEY; then
        echo -e "${GREEN}✅ HEX_API_KEY secret set successfully${NC}"
    else
        echo -e "${RED}❌ Failed to set GitHub secret automatically${NC}"
        echo "Please set it manually:"
        echo "1. Go to: https://github.com/hl/presto/settings/secrets/actions"
        echo "2. Click 'New repository secret'"
        echo "3. Name: HEX_API_KEY"
        echo "4. Value: $API_KEY"
    fi
else
    echo -e "${YELLOW}Manual setup required:${NC}"
    echo "1. Go to: https://github.com/hl/presto/settings/secrets/actions"
    echo "2. Click 'New repository secret'"
    echo "3. Name: HEX_API_KEY"
    echo "4. Value: $API_KEY"
    echo ""
    echo -e "${BLUE}API Key:${NC} $API_KEY"
    echo ""
    echo "⚠️  Save this key securely - it won't be shown again!"
fi

echo ""
echo -e "${BLUE}🛡️  Optional: Protected Environment Setup${NC}"
echo "=========================================="
echo "For additional security, you can create a protected environment:"
echo "1. Go to: https://github.com/hl/presto/settings/environments"
echo "2. Create environment: 'hex-publishing'"
echo "3. Add protection rules (required reviewers, wait timer)"
echo "4. Add HEX_API_KEY secret to the environment"
echo ""

echo -e "${BLUE}🧪 Testing Setup${NC}"
echo "==============="

echo "Testing package build..."
if mix hex.build > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Package builds successfully${NC}"
else
    echo -e "${RED}❌ Package build failed${NC}"
    echo "Please fix any issues before publishing"
    exit 1
fi

echo ""
echo -e "${GREEN}🎉 Setup Complete!${NC}"
echo "=================="
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "1. ✅ Hex API key generated and configured"
echo "2. ✅ GitHub secret set (HEX_API_KEY)"
echo "3. ✅ Package builds successfully"
echo ""
echo -e "${YELLOW}To publish a new version:${NC}"
echo "1. Run 'Prepare Release' workflow in GitHub Actions"
echo "2. Review and merge the generated PR"
echo "3. Create and push a version tag:"
echo "   git tag v0.2.0 && git push origin v0.2.0"
echo ""
echo -e "${BLUE}Documentation:${NC} docs/PUBLISHING.md"
echo -e "${BLUE}Workflows:${NC} .github/workflows/"
echo ""
echo "Happy publishing! 🚀"