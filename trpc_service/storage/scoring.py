"""Provider-neutral deterministic scoring used by fallback search adapters."""


def lexical_score(content: str, query: str) -> float:
    """Return token overlap without coupling one storage adapter to another."""

    query_terms = set(query.casefold().split())
    if not query_terms:
        return 0.0
    content_terms = set(content.casefold().split())
    return len(query_terms & content_terms) / len(query_terms)
