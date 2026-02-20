<a href="https://glama.ai/mcp/servers/@Yashkashte5/youtube-mcp">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@Yashkashte5/youtube-mcp/badge" />
</a>
# YouTube MCP Server

Production-grade Model Context Protocol (MCP) server for YouTube intelligence. Extract, analyze, and automate insights from YouTube channels and videos using the official YouTube Data API v3.

Built for AI agents, automation pipelines, and content intelligence workflows.

---

## What This Does

This MCP server exposes **16 specialized tools** for YouTube analytics and automation:

- **Channel Intelligence** — subscriber counts, video lists, upload patterns, topic analysis
- **Video Analytics** — detailed metadata, engagement metrics, performance comparison
- **Content Optimization** — SEO scoring, tag analysis, thumbnail evaluation
- **Audience Insights** — comment extraction, keyword analysis, sentiment signals
- **Trend Discovery** — trending video tracking, competitive benchmarking

Built with **zero scraping** — uses only official YouTube Data API v3 endpoints for reliability and quota efficiency.

---

## Prerequisites

### YouTube Data API Key

You **must** have a YouTube Data API v3 key to use this server.

**Get your API key:**

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select existing)
3. Enable **YouTube Data API v3**:
   - Navigate to **APIs & Services** → **Library**
   - Search for "YouTube Data API v3"
   - Click **Enable**
4. Create credentials:
   - Go to **APIs & Services** → **Credentials**
   - Click **Create Credentials** → **API Key**
   - Copy your API key

**Quota:** Free tier provides 10,000 units/day (sufficient for ~100-500 tool calls depending on complexity).

---

## Installation

### Step 1: Clone the repository

```bash
git clone https://github.com/Yashkashte5/Youtube-MCP.git
cd Youtube-MCP
```

### Step 2: Install dependencies

```bash
pip install -r requirements.txt
```

**Dependencies installed:**
- `mcp` — MCP protocol server framework
- `requests` — YouTube Data API v3 HTTP client
- `youtube-transcript-api` — transcript fetching (no OAuth)
- `Pillow` — thumbnail image analysis
- `nltk` — natural language processing for comment keywords
- `python-dotenv` — environment variable management

### Step 3: Configure API key

Create a `.env` file in the project root:

```bash
YOUTUBE_API_KEY=YOUR_API_KEY_HERE
```

**Important:** Never commit `.env` to git. It's already in `.gitignore`.

---

## Running the Server

### Standalone (for testing)

```bash
python server.py
```

### With Claude Desktop

Add to your `claude_desktop_config.json`:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "youtube-mcp": {
      "command": "python",
      "args": ["/absolute/path/to/Youtube-MCP/server.py"]
    }
  }
}
```

Replace `/absolute/path/to/Youtube-MCP/` with your actual project path.

**Then:** Fully quit and relaunch Claude Desktop.

---

## Tools Reference (16 Total)

### Channel Analysis

#### get_channel_overview
Subscriber count, total views, video count, creation date

![Demo](Demos/get_channel_overview-ezgif.com-video-to-gif-converter.gif)

#### get_channel_videos
Recent video list with views, likes, comments, duration

![Demo](Demos/get_channel_videos-ezgif.com-video-to-gif-converter.gif)

#### get_channel_topics
YouTube's topic categories (Gaming, Music, Tech, etc.)

![Demo](Demos/get_channel_topics-ezgif.com-video-to-gif-converter.gif)

#### compare_channels
Side-by-side channel comparison (max 5 channels)

![Demo](Demos/compare_channels-ezgif.com-video-to-gif-converter.gif)

#### get_upload_schedule
Posting frequency by day/hour, consistency score

![Demo](Demos/get_upload_schedule-ezgif.com-video-to-gif-converter.gif)

---

### Video Intelligence

#### get_video_details
Full metadata: title, description, tags, stats

![Demo](Demos/get_video_details-ezgif.com-video-to-gif-converter.gif)

#### get_video_comments
Top comments sorted by relevance with like counts

![Demo](Demos/get_video_comments-ezgif.com-video-to-gif-converter.gif)

#### get_video_transcript
Auto-generated or manual transcripts for NLP

![Demo](Demos/get_video_transcript-ezgif.com-video-to-gif-converter.gif)

#### compare_videos
Performance comparison across multiple videos

![Demo](Demos/compare_videos-ezgif.com-video-to-gif-converter.gif)

#### get_top_videos
Channel's best performers by views/likes/engagement

![Demo](Demos/get_top_videos-ezgif.com-video-to-gif-converter.gif)

---

### SEO & Optimization

#### analyze_thumbnail
Thumbnail URL, resolution, file size metadata

![Demo](Demos/analyze_thumbnail-ezgif.com-video-to-gif-converter.gif)

#### get_video_seo_score
0-100 score: title, description, tags, thumbnail quality

![Demo](Demos/get_video_seo_score-ezgif.com-video-to-gif-converter.gif)

#### get_tag_analysis
Tag frequency and correlation with video performance

![Demo](Demos/get_tag_analysis-ezgif.com-video-to-gif-converter.gif)

#### get_trending_videos
Popular videos by region/category (Music, Gaming, Movies)

![Demo](Demos/get_trending_videos-ezgif.com-video-to-gif-converter.gif)

---

### Advanced Analytics

#### get_engagement_stats
Like rate, comment rate, engagement rate per video

![Demo](Demos/get_engagement_stats-ezgif.com-video-to-gif-converter.gif)

#### get_comment_keywords
Most frequent words in comments (stopwords filtered)

![Demo](Demos/get_comment_keywords-ezgif.com-video-to-gif-converter.gif)

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  AI Agent / Claude Desktop / MCP Client             │
└──────────────────────┬──────────────────────────────┘
                       │ MCP Protocol
┌──────────────────────▼──────────────────────────────┐
│  server.py                                          │
│  ├─ Tool registration & MCP handlers                │
│  └─ JSON schema definitions                         │
└──────────────────────┬──────────────────────────────┘
                       │ Function calls
┌──────────────────────▼──────────────────────────────┐
│  main.py                                            │
│  ├─ 16 tool implementations                         │
│  ├─ YouTube Data API v3 integration                 │
│  ├─ Transcript API                                  │
│  └─ Data normalization & error handling             │
└──────────────────────┬──────────────────────────────┘
                       │ HTTPS
┌──────────────────────▼──────────────────────────────┐
│  YouTube Data API v3                                │
│  ├─ channels.list                                   │
│  ├─ videos.list                                     │
│  ├─ playlistItems.list                              │
│  ├─ commentThreads.list                             │
│  └─ Public data only                                │
└─────────────────────────────────────────────────────┘
```

**Design principles:**
- **MCP = Data layer** — deterministic, normalized JSON outputs
- **AI = Reasoning layer** — interprets data, generates insights
- **No scraping** — official API only for reliability
- **Quota efficient** — uses `playlistItems.list` instead of expensive `search.list`

---

## Project Structure

```
Youtube-MCP/
├── server.py              # MCP server, tool registration, schema definitions
├── main.py                # Tool implementations, YouTube API integration
├── requirements.txt       
├── .env                   # API key 
├── .gitignore
├── Demos/                 
└── README.md
```

---

## Example Use Cases

**AI Agents**
- Automated YouTube research assistants
- Content strategy recommendation systems
- Competitive intelligence bots

**Analytics Pipelines**
- Channel performance dashboards
- Engagement trend tracking
- Audience sentiment analysis

**Content Optimization**
- SEO audit automation
- Tag optimization recommendations
- Upload schedule optimization

**Research**
- YouTube ecosystem analysis
- Creator behavior patterns
- Viral content dynamics

---

## Contributing

Contributions welcome. Focus areas:

- **Tool reliability** — edge case handling, error messages
- **Performance** — quota optimization, caching strategies
- **New tools** — playlist analytics, growth metrics, predictions
- **Documentation** — usage examples, best practices

**To contribute:**
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-tool`)
3. Commit changes (`git commit -m 'Add new tool: X'`)
4. Push to branch (`git push origin feature/new-tool`)
5. Open a Pull Request

---

## License

MIT License — see LICENSE file for details.

---

## Author

**Yash Kashte**

GitHub: [@Yashkashte5](https://github.com/Yashkashte5)  
Project: [Youtube-MCP](https://github.com/Yashkashte5/Youtube-MCP)
