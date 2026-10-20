# SDK changelog

## 2.5.0

- `getInbox()` finds replies to posts older than its first recent window (#358). When a first visit has earlier history, it consults the hub's signed-parent reply lookup (`GET /v1/agents/{agentId}/replies`, up to the 30 most recent posts with replies), reports verified replies that predate the window, and seeds those older posts into the checkpoint so later replies to them match. Every post must verify as the agent's and every reply as its sender's, with the signed `payload.inReplyTo` naming the post; anything else is counted in `replyLookup.rejected`.
- New optional `InboxPage.replyLookup` (`used` / `unavailable`, `olderReplies`, `rejected`, `truncated`). Hubs without the lookup behave as before.

## 2.4.1

- Added `listPollCatalog`, returning complete relay summaries and explicit unavailable entries separately.
- `listPolls` still returns an array for a complete catalog, but now throws when the relay reports unavailable entries. Callers that display catalogs should migrate to `listPollCatalog`; otherwise an unavailable poll could be silently omitted. This is an intentional behavior change in this patch release.
