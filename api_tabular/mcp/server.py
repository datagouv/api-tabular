"""
MCP server implementation for api_tabular.
"""

import asyncio
import json
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolResult,
    ListResourcesResult,
    ListToolsResult,
    Resource,
    ResourceContents,
    TextContent,
    TextResourceContents,
    Tool,
)

from api_tabular.core.data_access import DataAccessor
from api_tabular.core.query_builder import QueryBuilder

# Configuration file path
RESOURCES_CONFIG_PATH = Path(__file__).parent / "data" / "mcp_available_resources.json"


class TabularMCPServer:
    """MCP server for accessing tabular data."""

    def __init__(self):
        self.server = Server("api-tabular-mcp")
        self.data_accessor = None
        self.query_builder = QueryBuilder()
        self.resources_config = self._load_resources_config()
        self._setup_handlers()

    def _load_resources_config(self) -> list[dict[str, any]]:
        """Load accessible resources configuration."""
        try:
            with RESOURCES_CONFIG_PATH.open("r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"⚠️  Configuration file not found: {RESOURCES_CONFIG_PATH}")
            return []
        except json.JSONDecodeError as e:
            print(f"⚠️  Invalid JSON in configuration file: {e}")
            return []

    def _setup_handlers(self):
        """Setup MCP server handlers."""
        self.server.list_tools()(self._list_tools)
        self.server.call_tool()(self._call_tool)
        self.server.list_resources()(self._list_resources)
        self.server.read_resource()(self._read_resource)

    async def _list_tools(self) -> ListToolsResult:
        """List available MCP tools."""
        return ListToolsResult(
            tools=[
                Tool(
                    name="list_accessible_resources",
                    description="Browse all available datasets and resources",
                    inputSchema={"type": "object", "properties": {}},
                ),
                Tool(
                    name="ask_data_question",
                    description="Ask natural language questions about available datasets and get data results",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "question": {
                                "type": "string",
                                "description": "Natural language question about the data",
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of results to return (default: auto-determined)",
                                "default": 0,  # 0 means auto-determine
                                "minimum": 1,
                                "maximum": 100,
                            },
                        },
                        "required": ["question"],
                    },
                ),
            ]
        )

    async def _call_tool(self, name: str, arguments: dict[str, Any]) -> CallToolResult:
        """Handle tool calls."""
        try:
            if name == "list_accessible_resources":
                return await self._list_accessible_resources(arguments)
            elif name == "ask_data_question":
                return await self._ask_data_question(arguments)
            else:
                return CallToolResult(
                    content=[TextContent(type="text", text=f"Unknown tool: {name}")], isError=True
                )
        except Exception as e:
            return CallToolResult(
                content=[TextContent(type="text", text=f"Error: {str(e)}")], isError=True
            )

    async def _list_accessible_resources(self, arguments: dict[str, Any]) -> CallToolResult:
        """List accessible resources from configuration."""
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(self.resources_config, indent=2))]
        )

    def _extract_keywords(self, question: str) -> list[str]:
        """Extract keywords from a natural language question."""
        import re

        # Convert to lowercase and remove punctuation
        text = re.sub(r"[^\w\s]", " ", question.lower())

        # Minimal French stop words (only the most common)
        stop_words = {
            "dans",
            "les",
            "la",
            "le",
            "des",
            "du",
            "de",
            "et",
            "ou",
            "avec",
            "pour",
            "par",
            "sur",
            "sous",
            "est",
            "sont",
            "que",
            "qui",
            "quoi",
            "où",
            "comment",
            "pourquoi",
        }

        # Split into words and filter out stop words and short words
        words = [word for word in text.split() if len(word) > 2 and word not in stop_words]

        return words

    def _calculate_match_score(self, keywords: list[str], dataset: dict, resource: dict) -> float:
        """Calculate how well a resource matches the given keywords using simple text similarity."""
        score = 0.0

        # Combine all searchable text
        searchable_text = f"{dataset.get('name', '')} {resource.get('name', '')}".lower()

        # Simple keyword matching - count occurrences
        for keyword in keywords:
            # Exact matches get higher score
            if keyword in searchable_text:
                score += 1.0
            # Partial matches get lower score
            elif any(keyword in word for word in searchable_text.split()):
                score += 0.5

        # Don't normalize by text length - this penalizes longer, more specific names
        # Instead, give bonus points for multiple keyword matches
        if score > 0:
            # Bonus for multiple keyword matches
            matched_keywords = sum(1 for keyword in keywords if keyword in searchable_text)
            if matched_keywords > 1:
                score += matched_keywords * 0.2

        return score

    def _find_matching_resource(self, keywords: list[str]) -> dict | None:
        """Find the best matching resource based on keywords."""
        best_match = None
        best_score = 0.0

        for dataset in self.resources_config:
            for resource in dataset["resources"]:
                score = self._calculate_match_score(keywords, dataset, resource)
                if score > best_score:
                    best_score = score
                    best_match = {"dataset": dataset, "resource": resource, "score": score}
                elif score == best_score and score > 0:
                    # If scores are equal, prioritize metro files
                    if (
                        "sg-metro-opendata" in resource["name"]
                        and "sg-metro-opendata" not in best_match["resource"]["name"]
                    ):
                        best_match = {"dataset": dataset, "resource": resource, "score": score}

        # Debug: Print top matches for debugging
        print("   🔍 Top matches found:")
        matches = []
        metro_matches = []
        for dataset in self.resources_config:
            for resource in dataset["resources"]:
                score = self._calculate_match_score(keywords, dataset, resource)
                if score > 0.05:  # Show matches with score > 0.05
                    matches.append((score, dataset["name"], resource["name"]))
                # Specifically track metro files
                if "sg-metro-opendata" in resource["name"]:
                    metro_matches.append((score, dataset["name"], resource["name"]))

        # Sort by score descending and show top 5
        matches.sort(reverse=True)
        for i, (score, dataset_name, resource_name) in enumerate(matches[:5]):
            print(f"     {i+1}. {resource_name} (score: {score:.3f}) - {dataset_name}")

        # Show metro-specific matches
        if metro_matches:
            print("   🚇 Metro files found:")
            metro_matches.sort(reverse=True)
            for i, (score, dataset_name, resource_name) in enumerate(metro_matches[:3]):
                print(f"     Metro {i+1}. {resource_name} (score: {score:.3f}) - {dataset_name}")
        else:
            print("   🚇 No metro files found in resources!")

        # Only return matches with a reasonable score (lowered threshold for generic matching)
        return best_match if best_score > 0.1 else None

    def _build_query_from_question(self, question: str, best_match: dict) -> list[str]:
        """Build query parameters based on question intent."""
        query_parts = []

        # Look for aggregation patterns in the question
        if any(
            word in question.lower()
            for word in ["la plupart", "plus", "maximum", "minimum", "moyenne", "total", "nombre"]
        ):
            # This suggests we want aggregated data
            if "département" in question.lower() or "departement" in question.lower():
                query_parts.append("département__groupby=true")
                query_parts.append("département__count=true")
            elif "région" in question.lower() or "region" in question.lower():
                query_parts.append("région__groupby=true")
                query_parts.append("région__count=true")

        # Look for sorting patterns
        if "plus" in question.lower() and "haut" in question.lower():
            query_parts.append("département__sort=desc")
        elif "moins" in question.lower() and "bas" in question.lower():
            query_parts.append("département__sort=asc")

        return query_parts

    def _format_results(self, data: list[dict], best_match: dict) -> str:
        """Format query results for natural language response."""
        if not data:
            return f"Aucune donnée trouvée pour la ressource '{best_match['resource']['name']}' du dataset '{best_match['dataset']['name']}'."

        result = f"**Résultats de la recherche dans '{best_match['dataset']['name']}'**\n"
        result += f"*Ressource: {best_match['resource']['name']}*\n\n"

        if len(data) <= 10:
            # Show all results if few
            for i, row in enumerate(data, 1):
                result += f"{i}. {row}\n"
        else:
            # Show first 5 and mention total
            for i, row in enumerate(data[:5], 1):
                result += f"{i}. {row}\n"
            result += f"\n... et {len(data) - 5} autres résultats\n"

        return result

    async def _search_all_pages(
        self, data_accessor, resource, indexes, question, best_match, max_results: int = 1000
    ) -> list[dict]:
        """Search through all pages to get comprehensive results."""
        from api_tabular import config

        all_results = []
        page_size = config.PAGE_SIZE_MAX  # Use the configured max page size (50)
        offset = 0

        while len(all_results) < max_results:
            # Build query for current page
            query_params = self._build_query_from_question(question, best_match)

            # Build SQL query with current offset
            sql_query = self.query_builder.build_sql_query_string(
                query_params, best_match["resource"]["resource_id"], indexes, page_size, offset
            )

            # Execute query for current page
            data, total = await data_accessor.get_resource_data(resource, sql_query)

            if not data:  # No more data
                break

            all_results.extend(data)

            # If we got fewer results than page_size, we've reached the end
            if len(data) < page_size:
                break

            offset += page_size

            # Safety check to prevent infinite loops
            if offset > 10000:  # Max 10,000 records
                break

        # Return only the requested number of results
        return all_results[:max_results]

    def _determine_limit(self, question: str, default_limit: int = 20) -> int:
        """Simple logic to determine appropriate result limit based on question type."""
        question_lower = question.lower()

        # Specific lookups - return fewer results
        if any(
            word in question_lower
            for word in [
                "qu'est-ce que",
                "quand",
                "combien",
                "coordonnées",
                "adresse",
                "où se trouve",
            ]
        ):
            return 5

        # Aggregations - return more results to show patterns
        if any(
            word in question_lower
            for word in [
                "la plupart",
                "plus",
                "maximum",
                "minimum",
                "moyenne",
                "total",
                "nombre",
                "top",
                "plus haut",
                "plus bas",
            ]
        ):
            return 50

        # Search queries - medium results
        if any(
            word in question_lower
            for word in ["trouver", "chercher", "rechercher", "montrer", "afficher", "voir"]
        ):
            return 30

        # Default
        return default_limit

    async def _ask_data_question(self, arguments: dict[str, Any]) -> CallToolResult:
        """Ask natural language questions about available data."""
        question = arguments["question"]
        user_limit = arguments.get("limit", 0)  # 0 means auto-determine

        # Auto-determine limit if user didn't specify one
        if user_limit == 0:
            limit = self._determine_limit(question)
        else:
            limit = user_limit

        try:
            # 1. Extract keywords from question
            keywords = self._extract_keywords(question)

            if not keywords:
                return CallToolResult(
                    content=[
                        TextContent(
                            type="text",
                            text="Je n'ai pas pu extraire de mots-clés pertinents de votre question. Pouvez-vous reformuler ?",
                        )
                    ],
                    isError=True,
                )

            # 2. Find best matching resource
            best_match = self._find_matching_resource(keywords)

            if not best_match:
                return CallToolResult(
                    content=[
                        TextContent(
                            type="text",
                            text=f"Aucun dataset correspondant trouvé pour les mots-clés: {', '.join(keywords)}. Essayez avec des termes plus généraux.",
                        )
                    ],
                    isError=True,
                )

            # 3. Use existing core logic to query data
            from aiohttp import ClientSession

            async with ClientSession() as session:
                data_accessor = DataAccessor(session)

                try:
                    # Get resource metadata
                    resource = await data_accessor.get_resource(
                        best_match["resource"]["resource_id"], ["parsing_table"]
                    )

                    # Get potential indexes
                    indexes = await data_accessor.get_potential_indexes(
                        best_match["resource"]["resource_id"]
                    )

                    # Debug: Print the resource and indexes
                    print(f"   🔍 Resource: {resource}")
                    print(f"   🔍 Indexes: {indexes}")

                    # Debug: Test direct table access
                    table_name = resource["parsing_table"]
                    print(f"   🔍 Testing direct table access: {table_name}")
                    try:
                        test_url = f"{self.pgrest_endpoint}/{table_name}?limit=1"
                        print(f"   🔍 Test URL: {test_url}")
                        async with httpx.AsyncClient() as client:
                            response = await client.get(test_url)
                            print(f"   🔍 Test response status: {response.status_code}")
                            if response.status_code == 200:
                                test_data = response.json()
                                print(
                                    f"   🔍 Test data sample: {test_data[:1] if test_data else 'No data'}"
                                )
                            else:
                                print(f"   🔍 Test response error: {response.text}")
                    except Exception as e:
                        print(f"   🔍 Test error: {e}")

                    # Simplified approach: just get data from the table directly
                    table_name = resource["parsing_table"]
                    print(f"   🔍 Simplified query: {table_name}?limit={limit}")
                    try:
                        url = f"{self.pgrest_endpoint}/{table_name}?limit={limit}"
                        async with httpx.AsyncClient() as client:
                            response = await client.get(url)
                            if response.status_code == 200:
                                data = response.json()
                                print(f"   🔍 Successfully retrieved {len(data)} records")
                            else:
                                print(f"   🔍 Error: {response.status_code} - {response.text}")
                                data = []
                    except Exception as e:
                        print(f"   🔍 Exception: {e}")
                        data = []

                    # 4. Format and return results
                    formatted_result = self._format_results(data, best_match)

                    return CallToolResult(content=[TextContent(type="text", text=formatted_result)])

                except Exception as e:
                    return CallToolResult(
                        content=[
                            TextContent(
                                type="text", text=f"Erreur lors de la requête des données: {str(e)}"
                            )
                        ],
                        isError=True,
                    )

        except Exception as e:
            return CallToolResult(
                content=[
                    TextContent(
                        type="text", text=f"Erreur lors du traitement de la question: {str(e)}"
                    )
                ],
                isError=True,
            )

    async def _list_resources(self) -> ListResourcesResult:
        """List available resources."""
        resources = []
        for dataset in self.resources_config:
            for resource in dataset.get("resources", []):
                resources.append(
                    Resource(
                        uri=f"resource://{resource['resource_id']}",
                        name=resource.get("name", resource["resource_id"]),
                        description=f"Resource from dataset: {dataset.get('name', 'Unknown')}",
                        mimeType="application/json",
                    )
                )
        return ListResourcesResult(resources=resources)

    async def _read_resource(self, uri: str) -> ResourceContents:
        """Read a specific resource."""
        # This would typically read resource data
        return TextResourceContents(
            uri=uri, mimeType="application/json", text="Resource content would be here"
        )

    async def run(self):
        """Run the MCP server."""
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="api-tabular-mcp",
                    server_version="1.0.0",
                    capabilities=self.server.get_capabilities(
                        notification_options=None, experimental_capabilities={}
                    ),
                ),
            )


def create_server() -> TabularMCPServer:
    """Create and return a new MCP server instance."""
    return TabularMCPServer()


async def main():
    """Main entry point for the MCP server."""
    server = create_server()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
