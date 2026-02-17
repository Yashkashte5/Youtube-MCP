"""
server.py — YouTube MCP Server
All MCP protocol logic, tool registration, and schema definitions live here.
"""

import json
import logging
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Tool,
    TextContent,
    CallToolResult,
    ListToolsResult,
)
import main

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("youtube-mcp")

# ---------------------------------------------------------------------------
# Server instance
# ---------------------------------------------------------------------------

app = Server("youtube-mcp")

# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS: list[Tool] = [

    # -----------------------------------------------------------------------
    # v1 Core Tools
    # -----------------------------------------------------------------------

    Tool(
        name="get_channel_overview",
        description=(
            "Returns a flat overview of a public YouTube channel. "
            "Includes subscriber count, total views, total videos, and creation date. "
            "Accepts a channel URL in @handle or /channel/UCxxxx format."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "channel_url": {
                    "type": "string",
                    "description": "YouTube channel URL. Supported formats: https://www.youtube.com/@handle or https://www.youtube.com/channel/UCxxxx",
                }
            },
            "required": ["channel_url"],
        },
    ),

    Tool(
        name="get_channel_videos",
        description=(
            "Returns a list of recent public videos from a channel, "
            "with per-video stats: views, likes, comments, duration. "
            "Use this as your primary dataset tool for channel analysis. "
            "Uses the uploads playlist internally — no quota-expensive search.list."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "channel_url": {
                    "type": "string",
                    "description": "YouTube channel URL. Supported formats: https://www.youtube.com/@handle or https://www.youtube.com/channel/UCxxxx",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of videos to return. Defaults to 50.",
                    "default": 50,
                    "minimum": 1,
                    "maximum": 200,
                },
            },
            "required": ["channel_url"],
        },
    ),

    Tool(
        name="get_video_details",
        description=(
            "Returns full metadata for a single video, including tags. "
            "Use this to deep-dive into one video after identifying it via get_channel_videos."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "video_id": {
                    "type": "string",
                    "description": "YouTube video ID (e.g. dQw4w9WgXcQ).",
                }
            },
            "required": ["video_id"],
        },
    ),

    Tool(
        name="get_video_comments",
        description=(
            "Returns top-level comments for a video, sorted by relevance. "
            "Includes author, comment text, like count, and publish date. "
            "Useful for audience sentiment and feedback analysis."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "video_id": {
                    "type": "string",
                    "description": "YouTube video ID (e.g. dQw4w9WgXcQ).",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of comments to return. Defaults to 100.",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 500,
                },
            },
            "required": ["video_id"],
        },
    ),

    Tool(
        name="get_video_transcript",
        description=(
            "Fetches the full transcript (auto-generated or manual) of a video. "
            "Returns concatenated transcript text, word count, and segment count. "
            "Useful for content analysis, summarization, and keyword extraction."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "video_id": {
                    "type": "string",
                    "description": "YouTube video ID (e.g. dQw4w9WgXcQ).",
                }
            },
            "required": ["video_id"],
        },
    ),

    Tool(
        name="analyze_thumbnail",
        description=(
            "Returns basic image metadata for a video's thumbnail: "
            "URL, resolution (WIDTHxHEIGHT), and file size in bytes. "
            "In v1 this is metadata only — no vision model analysis."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "video_id": {
                    "type": "string",
                    "description": "YouTube video ID (e.g. dQw4w9WgXcQ).",
                }
            },
            "required": ["video_id"],
        },
    ),

    # -----------------------------------------------------------------------
    # v2 Growth Tools
    # -----------------------------------------------------------------------

    Tool(
        name="get_trending_videos",
        description=(
            "Returns currently popular YouTube videos for a given region and category. "
            "NOTE: As of July 2025, YouTube removed its global Trending page. "
            "Results now come from category-specific charts (Music, Movies, Gaming). "
            "Use category_id 10 for Music, 20 for Gaming, 43 for Movies. "
            "category_id 0 returns a mixed set across all categories."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "region_code": {
                    "type": "string",
                    "description": "ISO 3166-1 alpha-2 country code (e.g. US, GB, IN). Defaults to US.",
                    "default": "US",
                },
                "category_id": {
                    "type": "string",
                    "description": "YouTube video category ID. Use '0' for all categories. Defaults to '0'.",
                    "default": "0",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of trending videos to return. Max 50. Defaults to 25.",
                    "default": 25,
                    "minimum": 1,
                    "maximum": 50,
                },
            },
            "required": [],
        },
    ),

    Tool(
        name="compare_videos",
        description=(
            "Side-by-side stats comparison for a list of video IDs (max 10). "
            "Returns per-video stats and declares winners by views, likes, comments, and engagement rate. "
            "Great for understanding why one video outperformed another."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "video_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of YouTube video IDs to compare (max 10).",
                    "minItems": 2,
                    "maxItems": 10,
                }
            },
            "required": ["video_ids"],
        },
    ),

    Tool(
        name="get_channel_topics",
        description=(
            "Returns the topic categories YouTube has associated with a channel. "
            "Returns human-readable topic names extracted from Wikipedia category URLs. "
            "Raw Freebase topic IDs are excluded as they have been deprecated since 2017 and are not human-readable."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "channel_url": {
                    "type": "string",
                    "description": "YouTube channel URL. Supported formats: https://www.youtube.com/@handle or https://www.youtube.com/channel/UCxxxx",
                }
            },
            "required": ["channel_url"],
        },
    ),

    Tool(
        name="compare_channels",
        description=(
            "Side-by-side overview comparison for multiple channels (max 5). "
            "Returns subscriber count, total views, and video count for each, "
            "plus declares winners in each category. Good for competitor analysis."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "channel_urls": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of YouTube channel URLs to compare (max 5).",
                    "minItems": 2,
                    "maxItems": 5,
                }
            },
            "required": ["channel_urls"],
        },
    ),

    Tool(
        name="get_top_videos",
        description=(
            "Returns a channel's top performing videos sorted by a chosen metric. "
            "Scans up to 200 recent videos and returns the top N. "
            "metric options: views | likes | comments | engagement_rate"
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "channel_url": {
                    "type": "string",
                    "description": "YouTube channel URL. Supported formats: https://www.youtube.com/@handle or https://www.youtube.com/channel/UCxxxx",
                },
                "metric": {
                    "type": "string",
                    "description": "Sort metric. One of: views, likes, comments, engagement_rate. Defaults to views.",
                    "enum": ["views", "likes", "comments", "engagement_rate"],
                    "default": "views",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of top videos to return. Defaults to 10.",
                    "default": 10,
                    "minimum": 1,
                    "maximum": 50,
                },
            },
            "required": ["channel_url"],
        },
    ),

    Tool(
        name="get_upload_schedule",
        description=(
            "Analyzes a channel's upload patterns. "
            "Returns posting frequency by day-of-week and hour-of-day, "
            "average days between uploads, consistency score, and best posting day/time. "
            "Useful for optimizing your own upload schedule."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "channel_url": {
                    "type": "string",
                    "description": "YouTube channel URL. Supported formats: https://www.youtube.com/@handle or https://www.youtube.com/channel/UCxxxx",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of recent videos to analyze. Defaults to 50.",
                    "default": 50,
                    "minimum": 10,
                    "maximum": 200,
                },
            },
            "required": ["channel_url"],
        },
    ),

    Tool(
        name="get_tag_analysis",
        description=(
            "Aggregates tags across a channel's videos and correlates them with view performance. "
            "Returns top tags by frequency and by average views. "
            "Useful for finding which tags drive the most traffic."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "channel_url": {
                    "type": "string",
                    "description": "YouTube channel URL. Supported formats: https://www.youtube.com/@handle or https://www.youtube.com/channel/UCxxxx",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of recent videos to analyze. Defaults to 50.",
                    "default": 50,
                    "minimum": 10,
                    "maximum": 200,
                },
            },
            "required": ["channel_url"],
        },
    ),

    Tool(
        name="get_video_seo_score",
        description=(
            "Checks a video's metadata against YouTube SEO best practices. "
            "Scores title length, description length/quality, tag count, and thumbnail presence. "
            "Returns an overall score (0–100) and per-dimension breakdown."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "video_id": {
                    "type": "string",
                    "description": "YouTube video ID (e.g. dQw4w9WgXcQ).",
                }
            },
            "required": ["video_id"],
        },
    ),

    Tool(
        name="get_engagement_stats",
        description=(
            "Computes per-video engagement metrics across a channel's recent videos. "
            "Returns average views, likes, comments, like rate %, comment rate %, "
            "overall engagement rate %, and the top engaging video. "
            "Great for benchmarking your channel's audience engagement health."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "channel_url": {
                    "type": "string",
                    "description": "YouTube channel URL. Supported formats: https://www.youtube.com/@handle or https://www.youtube.com/channel/UCxxxx",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of recent videos to analyze. Defaults to 50.",
                    "default": 50,
                    "minimum": 5,
                    "maximum": 200,
                },
            },
            "required": ["channel_url"],
        },
    ),

    Tool(
        name="get_comment_keywords",
        description=(
            "Extracts the most frequent meaningful words from a video's comments. "
            "Deterministic word frequency analysis — no LLM or sentiment model. "
            "Useful for understanding what topics and themes resonate with your audience."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "video_id": {
                    "type": "string",
                    "description": "YouTube video ID (e.g. dQw4w9WgXcQ).",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of comments to fetch for analysis. Defaults to 200.",
                    "default": 200,
                    "minimum": 10,
                    "maximum": 500,
                },
                "top_n": {
                    "type": "integer",
                    "description": "Number of top keywords to return. Defaults to 30.",
                    "default": 30,
                    "minimum": 5,
                    "maximum": 100,
                },
            },
            "required": ["video_id"],
        },
    ),

]

# ---------------------------------------------------------------------------
# MCP handlers
# ---------------------------------------------------------------------------

@app.list_tools()
async def list_tools() -> ListToolsResult:
    """Expose all registered tools to the MCP client."""
    return ListToolsResult(tools=TOOLS)


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> CallToolResult:
    """
    Route incoming tool calls to the correct function in main.py.
    Returns normalized JSON. All errors surfaced cleanly without crashing.
    """
    logger.info(f"Tool called: {name} | Arguments: {arguments}")

    try:
        result = _dispatch(name, arguments)
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))],
            isError=False,
        )
    except ValueError as e:
        logger.warning(f"ValueError in tool '{name}': {e}")
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps({"error": str(e)}))],
            isError=True,
        )
    except Exception as e:
        logger.error(f"Unexpected error in tool '{name}': {e}", exc_info=True)
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps({"error": f"Internal server error: {str(e)}"}) )],
            isError=True,
        )


def _dispatch(name: str, args: dict):
    """
    Pure dispatch table — maps tool names to main.py functions.
    No business logic here.
    """
    match name:

        # v1 Core
        case "get_channel_overview":
            return main.get_channel_overview(channel_url=args["channel_url"])

        case "get_channel_videos":
            return main.get_channel_videos(
                channel_url=args["channel_url"],
                limit=args.get("limit", 50),
            )

        case "get_video_details":
            return main.get_video_details(video_id=args["video_id"])

        case "get_video_comments":
            return main.get_video_comments(
                video_id=args["video_id"],
                limit=args.get("limit", 100),
            )

        case "get_video_transcript":
            return main.get_video_transcript(video_id=args["video_id"])

        case "analyze_thumbnail":
            return main.analyze_thumbnail(video_id=args["video_id"])

        # v2 Growth
        case "get_trending_videos":
            return main.get_trending_videos(
                region_code=args.get("region_code", "US"),
                category_id=args.get("category_id", "0"),
                limit=args.get("limit", 25),
            )

        case "compare_videos":
            return main.compare_videos(video_ids=args["video_ids"])

        case "get_channel_topics":
            return main.get_channel_topics(channel_url=args["channel_url"])

        case "compare_channels":
            return main.compare_channels(channel_urls=args["channel_urls"])

        case "get_top_videos":
            return main.get_top_videos(
                channel_url=args["channel_url"],
                metric=args.get("metric", "views"),
                limit=args.get("limit", 10),
            )

        case "get_upload_schedule":
            return main.get_upload_schedule(
                channel_url=args["channel_url"],
                limit=args.get("limit", 50),
            )

        case "get_tag_analysis":
            return main.get_tag_analysis(
                channel_url=args["channel_url"],
                limit=args.get("limit", 50),
            )

        case "get_video_seo_score":
            return main.get_video_seo_score(video_id=args["video_id"])

        case "get_engagement_stats":
            return main.get_engagement_stats(
                channel_url=args["channel_url"],
                limit=args.get("limit", 50),
            )

        case "get_comment_keywords":
            return main.get_comment_keywords(
                video_id=args["video_id"],
                limit=args.get("limit", 200),
                top_n=args.get("top_n", 30),
            )

        case _:
            raise ValueError(f"Unknown tool: '{name}'")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def run():
    """Start the MCP server over stdio."""
    logger.info("Starting youtube-mcp server (v2 — 16 tools)...")
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options(),
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(run())