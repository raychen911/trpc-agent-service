"""text-stats skill 脚本：统计文件的行数、词数、字符数。"""
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python3 text_stats.py <file>", file=sys.stderr)
        return 2
    path = sys.argv[1]
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    lines = content.count("\n") + (1 if content and not content.endswith("\n") else 0)
    words = len(content.split())
    chars = len(content)
    print(f"file: {path}")
    print(f"lines: {lines}")
    print(f"words: {words}")
    print(f"chars: {chars}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
