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

It is designed primarily for AI agent orchestration, YouTube automation workflows, and content intelligence research.

---

## Tools Available

## Channel Analysis

### get_channel_overview

Retrieves channel statistics and metadata.

![Preview](demos/get_channel_overview.gif)

### get_channel_videos

Lists videos from a channel.

![Preview](demos/get_channel_videos.gif)

### get_channel_topics

Extracts dominant channel themes.

![Preview](demos/get_channel_topics.gif)

### compare_channels

Comparative analytics between channels.

![Preview](demos/compare_channels.gif)

### get_upload_schedule

Identifies posting patterns.

![Preview](demos/get_upload_schedule.gif)

---

## Video Intelligence

### get_video_details

Metadata and engagement metrics extraction.

![Preview](demos/get_video_details.gif)

### get_video_comments

Comment extraction and analysis.

![Preview](demos/get_video_comments.gif)

### get_video_transcript

Transcript retrieval for NLP workflows.

![Preview](demos/get_video_transcript.gif)

### compare_videos

Performance comparison across videos.

![Preview](demos/compare_videos.gif)

### get_top_videos

Best performing content detection.

![Preview](demos/get_top_videos.gif)

---

## Optimization & Insights

### analyze_thumbnail

Thumbnail effectiveness analysis.

![Preview](demos/analyze_thumbnail.gif)

### get_video_seo_score

SEO evaluation for discoverability.

![Preview](demos/get_video_seo_score.gif)

### get_tag_analysis

Tag optimization insights.

![Preview](demos/get_tag_analysis.gif)

### get_trending_videos

Trending content discovery.

![Preview](demos/get_trending_videos.gif)

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

(Adjust based on your MCP environment.)

---

## Example Use Cases

* AI YouTube automation agents
* Content strategy analysis
* Competitor benchmarking
* Automated SEO recommendations
* Video transcript NLP pipelines

---

## Project Structure

```
Youtube-MCP/
│
├── server.py
├── tools/
├── demos/        # GIF demos for each tool
├── requirements.txt
└── README.md
```

---

## Roadmap

* Additional analytics tools
* Multi-platform intelligence integrations
* Improved agent orchestration support
* Real-time trend monitoring

---

## Contribution

Pull requests and improvements are welcome. Focus areas:

* Tool reliability
* Performance optimization
* Expanded analytics features

---

## Author

**Yash Kashte**
GitHub: [https://github.com/Yashkashte5](https://github.com/Yashkashte5)
