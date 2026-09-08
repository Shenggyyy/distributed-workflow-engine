"""Check repository Markdown file links and heading anchors without network calls."""

import re
from collections import Counter
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]


def prose(path: Path) -> str:
    return re.sub(r"(?ms)^```.*?^```[^\n]*", "", path.read_text(encoding="utf-8"))


def anchors(path: Path) -> set[str]:
    result: set[str] = set()
    seen: Counter[str] = Counter()
    for heading in re.findall(r"(?m)^#{1,6}\s+(.+?)\s*#*\s*$", prose(path)):
        heading = re.sub(r"<[^>]+>", "", heading).lower()
        slug = re.sub(r"[^\w\-\s]", "", heading).replace(" ", "-")
        count = seen[slug]
        seen[slug] += 1
        result.add(f"{slug}-{count}" if count else slug)
    result.update(re.findall(r'<a\s+(?:id|name)="([^"]+)"', prose(path)))
    return result


def main() -> None:
    files = sorted(ROOT.glob("README*.md")) + sorted((ROOT / "docs").rglob("*.md"))
    errors: list[str] = []
    checked = 0
    for path in files:
        for raw in re.findall(r"\]\(([^\s)]+)(?:\s+\"[^\"]*\")?\)", prose(path)):
            link = urlsplit(raw.strip("<>"))
            if link.scheme or link.netloc:
                continue
            target = (path.parent / unquote(link.path)).resolve() if link.path else path
            checked += 1
            if not target.is_relative_to(ROOT) or not target.exists():
                errors.append(f"{path.relative_to(ROOT)}: missing target {raw}")
            elif link.fragment and target.suffix == ".md":
                if unquote(link.fragment) not in anchors(target):
                    errors.append(f"{path.relative_to(ROOT)}: missing anchor {raw}")
    if errors:
        raise SystemExit("\n".join(errors))
    print(f"Validated {checked} local links/anchors in {len(files)} Markdown files.")


if __name__ == "__main__":
    main()
