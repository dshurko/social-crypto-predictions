import os
import time
from datetime import datetime

from atproto import Client


class BlueskyPostFetcher:
    def __init__(self):
        username = os.getenv("BSKY_USERNAME")
        password = os.getenv("BSKY_PASSWORD")
        if not username or not password:
            raise ValueError(
                "BSKY_USERNAME and BSKY_PASSWORD must be set as environment variables."
            )
        self.client = Client()
        self.client.login(username, password)

    def _build_query_params(
        self,
        query: str = "*",
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
        lang: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> dict:
        params = {
            "q": query,
            "lang": lang,
            "limit": limit,
        }
        if start_datetime:
            params["since"] = start_datetime.isoformat(timespec="seconds") + "Z"
        if end_datetime:
            params["until"] = end_datetime.isoformat(timespec="seconds") + "Z"
        if cursor:
            params["cursor"] = cursor
        return params

    def fetch_all_posts(
        self,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
        lang: str = "en",
        limit: int = 100,
        backoff_time: int = 5,
    ) -> list[dict]:
        known_posts = set()
        all_posts = []
        query = "*"
        cursor = None

        while True:
            try:
                params = self._build_query_params(
                    query=query,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                    lang=lang,
                    cursor=cursor,
                    limit=limit,
                )
                response = self.client.app.bsky.feed.search_posts(params=params)

                for post in response.posts:
                    if post.uri not in known_posts:
                        known_posts.add(post.uri)
                        all_posts.append(
                            {
                                "text": post.record.text,
                                "created_at": post.record.created_at,
                                "author_handle": post.author.handle,
                                "author_display_name": post.author.display_name,
                                "uri": post.uri,
                                "tags": getattr(post.record, "tags", None),
                                "like_count": post.like_count,
                                "repost_count": post.repost_count,
                                "reply_count": post.reply_count,
                                "quote_count": post.quote_count,
                            }
                        )

                if not response.cursor:
                    break
                cursor = response.cursor

            except Exception as e:
                print(f"Error during API request: {e}")
                print(f"Waiting for {backoff_time} seconds before retrying...")
                time.sleep(backoff_time)
                continue

        return all_posts
