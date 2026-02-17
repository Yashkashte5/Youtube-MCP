"""
main.py — YouTube MCP Tool Logic
All tools and YouTube Data API v3 integration live here.

Tools:
  v1 Core:
    1.  get_channel_overview
    2.  get_channel_videos
    3.  get_video_details
    4.  get_video_comments
    5.  get_video_transcript
    6.  analyze_thumbnail

  v2 Growth:
    7.  get_trending_videos
    8.  compare_videos
    9.  get_channel_topics
    10. compare_channels
    11. get_top_videos
    12. get_upload_schedule
    13. get_tag_analysis
    14. get_video_seo_score
    15. get_engagement_stats
    16. get_comment_keywords
"""

import re
import os
import statistics
import requests
from pathlib import Path
from collections import Counter
from urllib.parse import urlparse
from datetime import datetime
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Load .env from the same folder as this file — works regardless of cwd
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
BASE_URL = "https://www.googleapis.com/youtube/v3"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get(endpoint: str, params: dict) -> dict:
    """Thin wrapper around requests.get with shared API key and error handling."""
    params["key"] = API_KEY
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def resolve_channel_id(channel_url: str) -> str:
    """
    Resolve a YouTube channel URL to a canonical channel ID (UCxxxx).

    Supported formats:
      - https://www.youtube.com/@handle
      - https://www.youtube.com/channel/UCxxxx
    """
    parsed = urlparse(channel_url)
    path = parsed.path.rstrip("/")

    # Direct channel ID — fast path, no API call needed
    match = re.match(r"^/channel/(UC[\w-]+)$", path)
    if match:
        return match.group(1)

    # Handle-based URL (@handle)
    handle_match = re.match(r"^/@([\w.-]+)$", path)
    if handle_match:
        handle = handle_match.group(1)
        data = _get("channels", {
            "part": "id",
            "forHandle": handle,
            "maxResults": 1,
        })
        items = data.get("items", [])
        if not items:
            raise ValueError(f"No channel found for handle: @{handle}")
        return items[0]["id"]

    raise ValueError(
        f"Unsupported channel URL format: {channel_url}. "
        "Use https://www.youtube.com/@handle or https://www.youtube.com/channel/UCxxxx"
    )


def _get_uploads_playlist_id(channel_id: str) -> str:
    """Return the uploads playlist ID for a channel."""
    data = _get("channels", {
        "part": "contentDetails",
        "id": channel_id,
    })
    items = data.get("items", [])
    if not items:
        raise ValueError(f"Channel not found: {channel_id}")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def _parse_duration(iso_duration: str) -> int:
    """Convert ISO 8601 duration string (e.g. PT4M13S) to total seconds (int). Stdlib only."""
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso_duration or "")
    if not match:
        return 0
    h, m, s = (int(x or 0) for x in match.groups())
    return h * 3600 + m * 60 + s


def _safe_int(value) -> int:
    """Safely coerce a value to int, returning 0 on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_float(value, decimals: int = 4) -> float:
    """Safely coerce a value to float, returning 0.0 on failure."""
    try:
        return round(float(value), decimals)
    except (TypeError, ValueError):
        return 0.0


def _thumbnail_url(thumbnails: dict) -> str:
    """Pick the highest-resolution thumbnail available."""
    for quality in ("maxres", "standard", "high", "medium", "default"):
        if quality in thumbnails:
            return thumbnails[quality]["url"]
    return ""


def _fetch_videos_for_channel(channel_url: str, limit: int = 50) -> list:
    """
    Shared internal fetch — returns full normalized video list including tags.
    Used by multiple growth tools to avoid code duplication.
    """
    channel_id = resolve_channel_id(channel_url)
    uploads_playlist_id = _get_uploads_playlist_id(channel_id)

    video_ids = []
    next_page_token = None

    while len(video_ids) < limit:
        batch_size = min(50, limit - len(video_ids))
        params = {
            "part": "contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": batch_size,
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        data = _get("playlistItems", params)
        for item in data.get("items", []):
            vid_id = item.get("contentDetails", {}).get("videoId")
            if vid_id:
                video_ids.append(vid_id)

        next_page_token = data.get("nextPageToken")
        if not next_page_token or len(video_ids) >= limit:
            break

    if not video_ids:
        return []

    videos = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        data = _get("videos", {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(batch),
        })
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            content = item.get("contentDetails", {})
            stats = item.get("statistics", {})
            videos.append({
                "video_id": item["id"],
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "tags": snippet.get("tags", []),
                "published_at": snippet.get("publishedAt", ""),
                "duration_seconds": _parse_duration(content.get("duration", "PT0S")),
                "views": _safe_int(stats.get("viewCount", 0)),
                "likes": _safe_int(stats.get("likeCount", 0)),
                "comments": _safe_int(stats.get("commentCount", 0)),
                "thumbnail_url": _thumbnail_url(snippet.get("thumbnails", {})),
            })

    return videos


# ===========================================================================
# v1 CORE TOOLS
# ===========================================================================

# ---------------------------------------------------------------------------
# Tool 1 — get_channel_overview
# ---------------------------------------------------------------------------

def get_channel_overview(channel_url: str) -> dict:
    """Return a flat overview of a public YouTube channel."""
    channel_id = resolve_channel_id(channel_url)

    data = _get("channels", {
        "part": "snippet,statistics",
        "id": channel_id,
    })

    items = data.get("items", [])
    if not items:
        raise ValueError(f"No data returned for channel: {channel_id}")

    item = items[0]
    snippet = item.get("snippet", {})
    stats = item.get("statistics", {})

    return {
        "channel_id": channel_id,
        "title": snippet.get("title", ""),
        "description": snippet.get("description", ""),
        "subscriber_count": _safe_int(stats.get("subscriberCount", 0)),
        "total_views": _safe_int(stats.get("viewCount", 0)),
        "total_videos": _safe_int(stats.get("videoCount", 0)),
        "created_at": snippet.get("publishedAt", ""),
        "thumbnail_url": _thumbnail_url(snippet.get("thumbnails", {})),
    }


# ---------------------------------------------------------------------------
# Tool 2 — get_channel_videos
# ---------------------------------------------------------------------------

def get_channel_videos(channel_url: str, limit: int = 50) -> list:
    """Return a list of recent public videos from a channel with per-video stats."""
    videos = _fetch_videos_for_channel(channel_url, limit)
    return [
        {k: v for k, v in video.items() if k != "tags"}
        for video in videos
    ]


# ---------------------------------------------------------------------------
# Tool 3 — get_video_details
# ---------------------------------------------------------------------------

def get_video_details(video_id: str) -> dict:
    """Return detailed metadata for a single video, including tags."""
    data = _get("videos", {
        "part": "snippet,contentDetails,statistics",
        "id": video_id,
    })

    items = data.get("items", [])
    if not items:
        raise ValueError(f"No video found for ID: {video_id}")

    item = items[0]
    snippet = item.get("snippet", {})
    content = item.get("contentDetails", {})
    stats = item.get("statistics", {})

    return {
        "video_id": video_id,
        "title": snippet.get("title", ""),
        "description": snippet.get("description", ""),
        "tags": snippet.get("tags", []),
        "published_at": snippet.get("publishedAt", ""),
        "duration_seconds": _parse_duration(content.get("duration", "PT0S")),
        "views": _safe_int(stats.get("viewCount", 0)),
        "likes": _safe_int(stats.get("likeCount", 0)),
        "comments": _safe_int(stats.get("commentCount", 0)),
        "thumbnail_url": _thumbnail_url(snippet.get("thumbnails", {})),
    }


# ---------------------------------------------------------------------------
# Tool 4 — get_video_comments
# ---------------------------------------------------------------------------

def get_video_comments(video_id: str, limit: int = 100) -> dict:
    """Return top-level comments for a video, sorted by relevance."""
    video_data = _get("videos", {"part": "statistics", "id": video_id})
    video_items = video_data.get("items", [])
    total_count = 0
    if video_items:
        total_count = _safe_int(video_items[0].get("statistics", {}).get("commentCount", 0))

    comments = []
    next_page_token = None

    while len(comments) < limit:
        batch_size = min(100, limit - len(comments))
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": batch_size,
            "order": "relevance",
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        data = _get("commentThreads", params)
        for item in data.get("items", []):
            top = item.get("snippet", {}).get("topLevelComment", {})
            s = top.get("snippet", {})
            comments.append({
                "author": s.get("authorDisplayName", ""),
                "text": s.get("textDisplay", ""),
                "like_count": _safe_int(s.get("likeCount", 0)),
                "published_at": s.get("publishedAt", ""),
            })

        next_page_token = data.get("nextPageToken")
        if not next_page_token or len(comments) >= limit:
            break

    return {
        "video_id": video_id,
        "total_comment_count": total_count,
        "returned_comment_count": len(comments),
        "comments": comments,
    }


# ---------------------------------------------------------------------------
# Tool 5 — get_video_transcript
# ---------------------------------------------------------------------------

def get_video_transcript(video_id: str) -> dict:
    """
    Fetch the auto-generated or manual transcript for a video.
    Uses youtube-transcript-api >= 1.0.0 instance-based API.
    """
    try:
        ytt = YouTubeTranscriptApi()
        fetched = ytt.fetch(video_id)
        raw = fetched.to_raw_data()
    except Exception as e:
        error_msg = str(e)
        if "PoToken" in error_msg:
            raise ValueError(
                f"YouTube requires bot-verification for video {video_id}. "
                "This affects some high-traffic videos. Try a different video."
            )
        raise ValueError(f"Could not fetch transcript for {video_id}: {error_msg}")

    segments = [seg.get("text", "") for seg in raw]
    transcript_text = " ".join(segments).strip()
    word_count = len(transcript_text.split()) if transcript_text else 0

    return {
        "video_id": video_id,
        "transcript_text": transcript_text,
        "word_count": word_count,
        "segment_count": len(raw),
    }


# ---------------------------------------------------------------------------
# Tool 6 — analyze_thumbnail
# ---------------------------------------------------------------------------

def analyze_thumbnail(video_id: str) -> dict:
    """Return basic image metadata for a video's thumbnail."""
    data = _get("videos", {"part": "snippet", "id": video_id})
    items = data.get("items", [])
    if not items:
        raise ValueError(f"No video found for ID: {video_id}")

    thumbnails = items[0].get("snippet", {}).get("thumbnails", {})
    thumbnail_url = _thumbnail_url(thumbnails)
    if not thumbnail_url:
        raise ValueError(f"No thumbnail URL found for video: {video_id}")

    head_resp = requests.head(thumbnail_url, timeout=10)
    file_size_bytes = _safe_int(head_resp.headers.get("Content-Length", 0))

    img_resp = requests.get(thumbnail_url, timeout=10)
    img_resp.raise_for_status()

    try:
        from PIL import Image
        from io import BytesIO
        img = Image.open(BytesIO(img_resp.content))
        width, height = img.size
        resolution = f"{width}x{height}"
    except Exception:
        resolution = "unknown"

    if file_size_bytes == 0:
        file_size_bytes = len(img_resp.content)

    return {
        "video_id": video_id,
        "thumbnail_url": thumbnail_url,
        "resolution": resolution,
        "file_size_bytes": file_size_bytes,
    }


# ===========================================================================
# v2 GROWTH TOOLS
# ===========================================================================

# ---------------------------------------------------------------------------
# Tool 7 — get_trending_videos
# ---------------------------------------------------------------------------

def get_trending_videos(region_code: str = "US", category_id: str = "0", limit: int = 25) -> list:
    """
    Return currently popular YouTube videos for a region and category.

    NOTE: As of July 21, 2025, YouTube deprecated its global Trending page.
    The mostPopular chart now pulls from category-specific charts
    (Music, Movies, Gaming). Use category_id to target a specific chart.

    Supported category_ids for meaningful results:
        10 = Music, 20 = Gaming, 43 = Movies & Entertainment
        0  = Mixed results across all categories (less reliable)
    """
    params = {
        "part": "snippet,contentDetails,statistics",
        "chart": "mostPopular",
        "regionCode": region_code.upper(),
        "maxResults": min(limit, 50),
    }
    if category_id != "0":
        params["videoCategoryId"] = category_id

    data = _get("videos", params)
    results = []

    for item in data.get("items", []):
        snippet = item.get("snippet", {})
        content = item.get("contentDetails", {})
        stats = item.get("statistics", {})
        results.append({
            "video_id": item["id"],
            "title": snippet.get("title", ""),
            "channel_title": snippet.get("channelTitle", ""),
            "published_at": snippet.get("publishedAt", ""),
            "duration_seconds": _parse_duration(content.get("duration", "PT0S")),
            "views": _safe_int(stats.get("viewCount", 0)),
            "likes": _safe_int(stats.get("likeCount", 0)),
            "comments": _safe_int(stats.get("commentCount", 0)),
            "thumbnail_url": _thumbnail_url(snippet.get("thumbnails", {})),
        })

    return results


# ---------------------------------------------------------------------------
# Tool 8 — compare_videos
# ---------------------------------------------------------------------------

def compare_videos(video_ids: list) -> dict:
    """Side-by-side stats comparison for up to 10 video IDs."""
    if not video_ids:
        raise ValueError("video_ids must not be empty.")
    video_ids = video_ids[:10]

    data = _get("videos", {
        "part": "snippet,contentDetails,statistics",
        "id": ",".join(video_ids),
    })

    videos = []
    for item in data.get("items", []):
        snippet = item.get("snippet", {})
        content = item.get("contentDetails", {})
        stats = item.get("statistics", {})
        views = _safe_int(stats.get("viewCount", 0))
        likes = _safe_int(stats.get("likeCount", 0))
        comments = _safe_int(stats.get("commentCount", 0))
        engagement_rate = _safe_float((likes + comments) / views * 100 if views > 0 else 0)
        videos.append({
            "video_id": item["id"],
            "title": snippet.get("title", ""),
            "published_at": snippet.get("publishedAt", ""),
            "duration_seconds": _parse_duration(content.get("duration", "PT0S")),
            "views": views,
            "likes": likes,
            "comments": comments,
            "engagement_rate_pct": engagement_rate,
        })

    def _winner(key):
        if not videos:
            return ""
        return max(videos, key=lambda v: v[key])["video_id"]

    return {
        "videos": videos,
        "winner_by_views": _winner("views"),
        "winner_by_likes": _winner("likes"),
        "winner_by_comments": _winner("comments"),
        "winner_by_engagement_rate": _winner("engagement_rate_pct"),
    }


# ---------------------------------------------------------------------------
# Tool 9 — get_channel_topics
# ---------------------------------------------------------------------------

def get_channel_topics(channel_url: str) -> dict:
    """
    Return the topic categories YouTube has associated with a channel.

    NOTE: topicIds returns raw Freebase IDs (e.g. /m/04rlf) which are not
    human-readable since Freebase was deprecated in 2017. Only topicCategories
    (Wikipedia URLs) are returned as they are actually meaningful.
    """
    channel_id = resolve_channel_id(channel_url)

    data = _get("channels", {
        "part": "snippet,topicDetails",
        "id": channel_id,
    })

    items = data.get("items", [])
    if not items:
        raise ValueError(f"No data returned for channel: {channel_id}")

    item = items[0]
    topic_details = item.get("topicDetails", {})
    raw_categories = topic_details.get("topicCategories", [])

    # Extract readable topic name from Wikipedia URL
    # e.g. "https://en.wikipedia.org/wiki/Gaming" -> "Gaming"
    readable_topics = [url.split("/wiki/")[-1].replace("_", " ") for url in raw_categories]

    return {
        "channel_id": channel_id,
        "title": item.get("snippet", {}).get("title", ""),
        "topics": readable_topics,
        "topic_category_urls": raw_categories,
    }


# ---------------------------------------------------------------------------
# Tool 10 — compare_channels
# ---------------------------------------------------------------------------

def compare_channels(channel_urls: list) -> dict:
    """Side-by-side overview comparison for up to 5 channels."""
    if not channel_urls:
        raise ValueError("channel_urls must not be empty.")
    channel_urls = channel_urls[:5]

    channels = [get_channel_overview(url) for url in channel_urls]

    def _winner(key):
        if not channels:
            return ""
        return max(channels, key=lambda c: c[key])["channel_id"]

    return {
        "channels": channels,
        "winner_by_subscribers": _winner("subscriber_count"),
        "winner_by_total_views": _winner("total_views"),
        "winner_by_video_count": _winner("total_videos"),
    }


# ---------------------------------------------------------------------------
# Tool 11 — get_top_videos
# ---------------------------------------------------------------------------

def get_top_videos(channel_url: str, metric: str = "views", limit: int = 10) -> list:
    """
    Return a channel's top performing videos sorted by a given metric.
    metric options: views | likes | comments | engagement_rate
    """
    valid_metrics = {"views", "likes", "comments", "engagement_rate"}
    if metric not in valid_metrics:
        raise ValueError(f"metric must be one of: {valid_metrics}")

    videos = _fetch_videos_for_channel(channel_url, limit=200)

    for v in videos:
        views = v["views"]
        v["engagement_rate_pct"] = _safe_float(
            (v["likes"] + v["comments"]) / views * 100 if views > 0 else 0
        )

    sort_key = "engagement_rate_pct" if metric == "engagement_rate" else metric
    sorted_videos = sorted(videos, key=lambda v: v[sort_key], reverse=True)[:limit]

    return [
        {
            "rank": idx + 1,
            "video_id": v["video_id"],
            "title": v["title"],
            "published_at": v["published_at"],
            "duration_seconds": v["duration_seconds"],
            "views": v["views"],
            "likes": v["likes"],
            "comments": v["comments"],
            "engagement_rate_pct": v["engagement_rate_pct"],
        }
        for idx, v in enumerate(sorted_videos)
    ]


# ---------------------------------------------------------------------------
# Tool 12 — get_upload_schedule
# ---------------------------------------------------------------------------

def get_upload_schedule(channel_url: str, limit: int = 50) -> dict:
    """
    Analyze upload patterns: posting frequency by day/hour,
    average gap between uploads, and consistency score.
    """
    videos = _fetch_videos_for_channel(channel_url, limit=limit)
    if not videos:
        raise ValueError("No videos found for this channel.")

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    posts_by_day = Counter()
    posts_by_hour = Counter()
    timestamps = []

    for v in videos:
        try:
            dt = datetime.fromisoformat(v["published_at"].replace("Z", "+00:00"))
            posts_by_day[day_names[dt.weekday()]] += 1
            posts_by_hour[f"{dt.hour:02d}"] += 1
            timestamps.append(dt)
        except Exception:
            continue

    avg_days = 0.0
    consistency_score = 0.0

    if len(timestamps) > 1:
        timestamps_sorted = sorted(timestamps)
        gaps = [
            (timestamps_sorted[i + 1] - timestamps_sorted[i]).days
            for i in range(len(timestamps_sorted) - 1)
        ]
        avg_days = _safe_float(sum(gaps) / len(gaps), 1)

        if gaps:
            mean_gap = sum(gaps) / len(gaps)
            try:
                std = statistics.stdev(gaps)
                consistency_score = _safe_float(max(0, 100 - (std / max(mean_gap, 1)) * 50), 1)
            except Exception:
                consistency_score = 0.0

    best_day = posts_by_day.most_common(1)[0][0] if posts_by_day else ""
    best_hour = posts_by_hour.most_common(1)[0][0] if posts_by_hour else ""

    return {
        "total_videos_analyzed": len(videos),
        "avg_days_between_uploads": avg_days,
        "consistency_score_pct": consistency_score,
        "posts_by_day": dict(sorted(posts_by_day.items(), key=lambda x: day_names.index(x[0]))),
        "posts_by_hour": dict(sorted(posts_by_hour.items())),
        "best_posting_day": best_day,
        "best_posting_hour": f"{best_hour}:00 UTC" if best_hour else "",
    }


# ---------------------------------------------------------------------------
# Tool 13 — get_tag_analysis
# ---------------------------------------------------------------------------

def get_tag_analysis(channel_url: str, limit: int = 50) -> dict:
    """
    Aggregate tags across a channel's videos and correlate with performance.
    Returns top tags by frequency and by average views.
    """
    videos = _fetch_videos_for_channel(channel_url, limit=limit)
    if not videos:
        raise ValueError("No videos found for this channel.")

    tag_views: dict = {}

    for v in videos:
        for tag in v.get("tags", []):
            tag_lower = tag.lower().strip()
            if tag_lower:
                tag_views.setdefault(tag_lower, []).append(v["views"])

    if not tag_views:
        return {
            "total_videos_analyzed": len(videos),
            "unique_tags": 0,
            "top_tags_by_frequency": [],
            "top_tags_by_avg_views": [],
        }

    tag_stats = [
        {
            "tag": tag,
            "count": len(view_list),
            "avg_views": _safe_float(sum(view_list) / len(view_list), 0),
        }
        for tag, view_list in tag_views.items()
    ]

    return {
        "total_videos_analyzed": len(videos),
        "unique_tags": len(tag_views),
        "top_tags_by_frequency": sorted(tag_stats, key=lambda x: x["count"], reverse=True)[:20],
        "top_tags_by_avg_views": sorted(tag_stats, key=lambda x: x["avg_views"], reverse=True)[:20],
    }


# ---------------------------------------------------------------------------
# Tool 14 — get_video_seo_score
# ---------------------------------------------------------------------------

def get_video_seo_score(video_id: str) -> dict:
    """
    Check a video's metadata against YouTube SEO best practices.
    Scores each dimension 0-100 and returns an overall score.
    """
    data = _get("videos", {
        "part": "snippet,contentDetails,statistics",
        "id": video_id,
    })
    items = data.get("items", [])
    if not items:
        raise ValueError(f"No video found for ID: {video_id}")

    item = items[0]
    snippet = item.get("snippet", {})
    title = snippet.get("title", "")
    description = snippet.get("description", "")
    tags = snippet.get("tags", [])
    thumbnail = _thumbnail_url(snippet.get("thumbnails", {}))

    checks = {}

    # Title length (ideal: 40–70 chars)
    tlen = len(title)
    if 40 <= tlen <= 70:
        checks["title_length"] = {"score": 100, "status": "great", "note": f"{tlen} chars (ideal: 40–70)"}
    elif 20 <= tlen < 40:
        checks["title_length"] = {"score": 70, "status": "ok", "note": f"{tlen} chars (a bit short, ideal: 40–70)"}
    elif tlen > 70:
        checks["title_length"] = {"score": 60, "status": "ok", "note": f"{tlen} chars (a bit long, ideal: 40–70)"}
    else:
        checks["title_length"] = {"score": 30, "status": "poor", "note": f"{tlen} chars (too short)"}

    # Description length (ideal: 200+ chars)
    dlen = len(description)
    if dlen >= 500:
        checks["description_length"] = {"score": 100, "status": "great", "note": f"{dlen} chars"}
    elif dlen >= 200:
        checks["description_length"] = {"score": 80, "status": "good", "note": f"{dlen} chars (ideal: 500+)"}
    elif dlen >= 50:
        checks["description_length"] = {"score": 50, "status": "ok", "note": f"{dlen} chars (ideal: 200+)"}
    else:
        checks["description_length"] = {"score": 10, "status": "poor", "note": f"{dlen} chars (missing or very short)"}

    # Tag count (ideal: 5–15)
    tcount = len(tags)
    if 5 <= tcount <= 15:
        checks["tag_count"] = {"score": 100, "status": "great", "note": f"{tcount} tags (ideal: 5–15)"}
    elif tcount > 15:
        checks["tag_count"] = {"score": 70, "status": "ok", "note": f"{tcount} tags (slightly over, ideal: 5–15)"}
    elif 1 <= tcount < 5:
        checks["tag_count"] = {"score": 50, "status": "ok", "note": f"{tcount} tags (too few, ideal: 5–15)"}
    else:
        checks["tag_count"] = {"score": 0, "status": "poor", "note": "No tags found"}

    # Thumbnail present
    if thumbnail:
        checks["thumbnail"] = {"score": 100, "status": "great", "note": "Thumbnail present"}
    else:
        checks["thumbnail"] = {"score": 0, "status": "poor", "note": "No thumbnail found"}

    # Description quality (links + timestamps)
    has_links = "http" in description.lower()
    has_chapters = bool(re.search(r"\d+:\d+", description))
    desc_score = 60
    desc_notes = []
    if has_links:
        desc_score += 20
        desc_notes.append("has links")
    if has_chapters:
        desc_score += 20
        desc_notes.append("has chapters/timestamps")
    checks["description_quality"] = {
        "score": min(desc_score, 100),
        "status": "great" if desc_score >= 90 else "good" if desc_score >= 70 else "ok",
        "note": ", ".join(desc_notes) if desc_notes else "no links or timestamps detected",
    }

    overall = round(sum(c["score"] for c in checks.values()) / len(checks))

    return {
        "video_id": video_id,
        "title": title,
        "overall_score": overall,
        "checks": checks,
    }


# ---------------------------------------------------------------------------
# Tool 15 — get_engagement_stats
# ---------------------------------------------------------------------------

def get_engagement_stats(channel_url: str, limit: int = 50) -> dict:
    """
    Compute per-video engagement metrics across a channel's recent videos.
    Returns averages, rates, and the top engaging video.
    """
    videos = _fetch_videos_for_channel(channel_url, limit=limit)
    if not videos:
        raise ValueError("No videos found for this channel.")

    enriched = []
    for v in videos:
        views = v["views"]
        likes = v["likes"]
        comments = v["comments"]
        like_rate = _safe_float(likes / views * 100 if views > 0 else 0)
        comment_rate = _safe_float(comments / views * 100 if views > 0 else 0)
        engagement_rate = _safe_float((likes + comments) / views * 100 if views > 0 else 0)
        enriched.append({
            "video_id": v["video_id"],
            "title": v["title"],
            "views": views,
            "likes": likes,
            "comments": comments,
            "like_rate_pct": like_rate,
            "comment_rate_pct": comment_rate,
            "engagement_rate_pct": engagement_rate,
        })

    n = len(enriched)
    avg_views = _safe_float(sum(v["views"] for v in enriched) / n, 0)
    avg_likes = _safe_float(sum(v["likes"] for v in enriched) / n, 0)
    avg_comments = _safe_float(sum(v["comments"] for v in enriched) / n, 0)
    avg_like_rate = _safe_float(sum(v["like_rate_pct"] for v in enriched) / n)
    avg_comment_rate = _safe_float(sum(v["comment_rate_pct"] for v in enriched) / n)
    avg_engagement = _safe_float(sum(v["engagement_rate_pct"] for v in enriched) / n)
    top = max(enriched, key=lambda v: v["engagement_rate_pct"])

    return {
        "total_videos_analyzed": n,
        "avg_views": avg_views,
        "avg_likes": avg_likes,
        "avg_comments": avg_comments,
        "avg_like_rate_pct": avg_like_rate,
        "avg_comment_rate_pct": avg_comment_rate,
        "avg_engagement_rate_pct": avg_engagement,
        "top_engaging_video": {
            "video_id": top["video_id"],
            "title": top["title"],
            "engagement_rate_pct": top["engagement_rate_pct"],
        },
        "videos": enriched,
    }


# ---------------------------------------------------------------------------
# Tool 16 — get_comment_keywords
# ---------------------------------------------------------------------------

def get_comment_keywords(video_id: str, limit: int = 200, top_n: int = 30) -> dict:
    """
    Extract most frequent meaningful words from a video's comments.
    Deterministic — no LLM, no sentiment model. Pure word frequency.
    Uses NLTK's stopwords corpus (179 English stopwords) for filtering.
    """
    import nltk
    nltk.download("stopwords", quiet=True)
    nltk.download("punkt_tab", quiet=True)
    from nltk.corpus import stopwords as nltk_stopwords
    from nltk.tokenize import word_tokenize
    STOPWORDS = set(nltk_stopwords.words("english"))

    comment_data = get_video_comments(video_id, limit=limit)
    all_text = " ".join(c["text"] for c in comment_data["comments"])

    # word_tokenize handles contractions, punctuation, and edge cases correctly
    tokens = word_tokenize(all_text.lower())
    filtered = [w for w in tokens if w.isalpha() and len(w) >= 3 and w not in STOPWORDS]
    counter = Counter(filtered)

    return {
        "video_id": video_id,
        "comments_analyzed": comment_data["returned_comment_count"],
        "top_keywords": [
            {"word": word, "count": count}
            for word, count in counter.most_common(top_n)
        ],
    }