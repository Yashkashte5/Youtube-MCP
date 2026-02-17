# YouTube MCP Server

Model Context Protocol (MCP) server providing structured tools for extracting, analyzing, and automating insights from YouTube channels and videos.

This project exposes YouTube intelligence capabilities as MCP-compatible tools so AI agents, automation pipelines, and analytics workflows can interact with YouTube data in a standardized way.

---

## Overview

This repository provides a modular MCP server that enables:

* Channel analytics
* Video metadata extraction
* Comments and transcript retrieval
* Thumbnail analysis
* SEO insights
* Trend tracking
* Engagement analytics
* Comment keyword intelligence

Designed primarily for AI agent orchestration, YouTube automation workflows, and content intelligence research.

---

## Tools Available (16 Total)

## Channel Analysis

### get_channel_overview

Retrieves channel statistics and metadata.

![Preview](Demos/get_channel_overview-ezgif.com-video-to-gif-converter.gif)

### get_channel_videos

Lists videos from a channel.

![Preview](Demos/get_channel_videos-ezgif.com-video-to-gif-converter.gif)

### get_channel_topics

Extracts dominant channel themes.

![Preview](Demos/get_channel_topics-ezgif.com-video-to-gif-converter.gif)

### compare_channels

Comparative analytics between channels.

![Preview](Demos/compare_channels-ezgif.com-video-to-gif-converter.gif)

### get_upload_schedule

Identifies posting patterns.

![Preview](Demos/get_upload_schedule-ezgif.com-video-to-gif-converter.gif)

---

## Video Intelligence

### get_video_details

Metadata and engagement metrics extraction.

![Preview](Demos/get_video_details-ezgif.com-video-to-gif-converter.gif)

### get_video_comments

Comment extraction and analysis.

![Preview](Demos/get_video_comments-ezgif.com-video-to-gif-converter.gif)

### get_video_transcript

Transcript retrieval for NLP workflows.

![Preview](Demos/get_video_transcript-ezgif.com-video-to-gif-converter.gif)

### compare_videos

Performance comparison across videos.

![Preview](Demos/compare_videos-ezgif.com-video-to-gif-converter.gif)

### get_top_videos

Best performing content detection.

![Preview](Demos/get_top_videos-ezgif.com-video-to-gif-converter.gif)

---

## Optimization, SEO & Trends

### analyze_thumbnail

Thumbnail effectiveness analysis.

![Preview](Demos/analyze_thumbnail-ezgif.com-video-to-gif-converter.gif)

### get_video_seo_score

SEO evaluation for discoverability.

![Preview](Demos/get_video_seo_score-ezgif.com-video-to-gif-converter.gif)

### get_tag_analysis

Tag optimization insights.

![Preview](Demos/get_tag_analysis-ezgif.com-video-to-gif-converter.gif)

### get_trending_videos

Trending content discovery.

![Preview](Demos/get_trending_videos-ezgif.com-video-to-gif-converter.gif)

---

## Advanced Analytics

### get_engagement_stats

Deep engagement metrics across videos.

![Preview](Demos/get_engagement_stats-ezgif.com-video-to-gif-converter.gif)

### get_comment_keywords

Extracts keyword intelligence from comments.

![Preview](Demos/get_comment_keywords-ezgif.com-video-to-gif-converter.gif)

---

## Installation

```bash
git clone https://github.com/Yashkashte5/Youtube-MCP.git
cd Youtube-MCP
pip install -r requirements.txt
```

---

## Running the MCP Server

```bash
python server.py
```

If integrated with an MCP runtime/client:

```bash
mcp run server.py
```

---

## Example Use Cases

* AI YouTube automation agents
* Content strategy analysis
* Competitor benchmarking
* Automated SEO recommendations
* Audience sentiment analysis
* Engagement analytics dashboards

---

## Project Structure

```
Youtube-MCP/
│
├── server.py
├── tools/
├── Demos/
├── requirements.txt
└── README.md
```

---

## Roadmap

* Expanded analytics tools
* Cross-platform video intelligence
* Stronger MCP agent orchestration integrations
* Real-time trend monitoring

---

## Contribution

Pull requests and improvements are welcome. Focus areas:

* Tool reliability
* Performance optimization
* Additional analytics capabilities

---

## Author

**Yash Kashte**
GitHub: [https://github.com/Yashkashte5](https://github.com/Yashkashte5)
