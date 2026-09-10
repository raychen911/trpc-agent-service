---
name: text-stats
description: 统计文本文件的行数、词数与字符数，并把结果写入输出文件。
---

# text-stats

统计指定文本文件的基本信息，适合演示 Skill 工作区执行流程。

## 用法

    python3 scripts/text_stats.py <file>

## 示例

统计 data.txt 并输出到 out/stats.txt：

    python3 scripts/text_stats.py data.txt > out/stats.txt

## 输出

- stdout：行数 / 词数 / 字符数汇总
