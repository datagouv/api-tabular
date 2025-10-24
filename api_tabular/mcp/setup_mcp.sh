#!/bin/bash
# Setup script for MCP server

echo "🚀 Setting up MCP server..."

# Check if we're in the right directory
if [ ! -f "api_tabular/mcp/server.py" ]; then
    echo "❌ Please run this script from the project root directory"
    exit 1
fi

# Check if Docker is running
if ! docker ps > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start Hydra database
echo "🗄️  Starting Hydra database..."
docker compose -f docker-compose-hydra.yml up -d

# Wait for database to be ready
echo "⏳ Waiting for database to be ready..."
sleep 5

# Test database connection
echo "🔍 Testing database connection..."
if curl -s http://localhost:8081/tables_index?limit=1 > /dev/null; then
    echo "✅ Database is accessible"
else
    echo "❌ Database is not accessible. Please check your setup."
    exit 1
fi

# Test MCP server
echo "🧪 Testing MCP server..."
if uv run python api_tabular/mcp/test_mcp.py > /dev/null 2>&1; then
    echo "✅ MCP server is working"
else
    echo "❌ MCP server test failed"
    exit 1
fi

echo "🎉 MCP server is ready!"
echo ""
echo "📋 Configuration for your chat agent:"
echo "Command: uv run python $(pwd)/api_tabular/mcp/server.py"
echo "Working Directory: $(pwd)"
echo "Environment: PGREST_ENDPOINT=http://localhost:8081"
echo ""
echo "🔧 Add this to your MCP client config:"
echo '{
  "mcpServers": {
    "api-tabular": {
      "command": "uv",
      "args": [
        "run",
        "python",
        "'$(pwd)'/api_tabular/mcp/server.py"
      ],
      "cwd": "'$(pwd)'"
    }
  }
}'
