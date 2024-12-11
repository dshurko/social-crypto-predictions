import os
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
        query: str,
        start_datetime: datetime | None,
        end_datetime: datetime | None,
        cursor: str | None,
        limit: int,
    ) -> dict:
        params = {
            "q": query,
            "limit": limit,
        }
        if start_datetime:
            params["since"] = start_datetime.isoformat(timespec="seconds") + "Z"
        if end_datetime:
            params["until"] = end_datetime.isoformat(timespec="seconds") + "Z"
        if cursor:
            params["cursor"] = cursor
        return params

    def fetch_posts_by_keywords(
        self,
        keywords: list[str],
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
        limit: int = 100,
    ) -> list[dict]:
        known_posts = set()
        all_posts = []

        for keyword in keywords:
            query = keyword.lower()
            cursor = None

            while True:
                try:
                    params = self._build_query_params(
                        query=query,
                        start_datetime=start_datetime,
                        end_datetime=end_datetime,
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
                    print(f"Error during API request for keyword '{keyword}': {e}")
                    break

        return all_posts
