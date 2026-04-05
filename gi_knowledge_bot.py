"""
消化器疾患 最新知見 定時投稿 Bot
====================================
- 1時間に1回、消化器疾患に関するトピックを投稿
- PubMed から関連論文を複数取得し、根拠文献として明示
- Gemini 2.5 Flash で最新知見を文献ベースで解説
- Discord Webhook で投稿
"""

import os
import json
import random
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import requests
import google.generativeai as genai

# ============================================================
# 設定
# ============================================================
GI_KNOWLEDGE_WEBHOOK_URL = os.environ["GI_KNOWLEDGE_WEBHOOK_URL"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]

genai.configure(api_key=GEMINI_API_KEY)
GEMINI_MODEL = "gemini-2.5-flash"

# 投稿済みトピック記録
POSTED_FILE = Path(__file__).parent / "posted_gi_knowledge.json"

# PubMed 検索設定
SEARCH_DAYS = 90    # 直近90日から検索（質の高い論文を広く拾う）
MAX_RESULTS = 30    # 各カテゴリから最大30件取得
PAPERS_PER_POST = 3  # 1投稿あたり引用する論文数

# ============================================================
# トピックカテゴリ（疾患・テーマ × 検索クエリ）
# 毎回ランダムに1つ選び、多様なトピックを紹介する
# ============================================================
TOPIC_CATEGORIES = [
    # --- IBD ---
    {
        "topic": "潰瘍性大腸炎の治療戦略",
        "query": '"Colitis, Ulcerative/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "IBD",
        "emoji": "🔥",
    },
    {
        "topic": "クローン病の病態と治療",
        "query": '"Crohn Disease/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "IBD",
        "emoji": "🔥",
    },
    {
        "topic": "IBDにおけるバイオマーカー",
        "query": '("Inflammatory Bowel Diseases"[MeSH]) AND ("Biomarkers"[MeSH]) AND "humans"[MeSH]',
        "field": "IBD",
        "emoji": "🔥",
    },
    {
        "topic": "IBDと生物学的製剤",
        "query": '("Inflammatory Bowel Diseases"[MeSH]) AND ("Biological Products/therapeutic use"[MeSH]) AND "humans"[MeSH]',
        "field": "IBD",
        "emoji": "🔥",
    },
    # --- 肝臓 ---
    {
        "topic": "MASLD/NAFLDの最新治療",
        "query": '"Non-alcoholic Fatty Liver Disease/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "肝臓",
        "emoji": "🫁",
    },
    {
        "topic": "肝細胞癌の診断と治療",
        "query": '"Carcinoma, Hepatocellular/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "肝臓",
        "emoji": "🫁",
    },
    {
        "topic": "肝硬変の管理",
        "query": '"Liver Cirrhosis/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "肝臓",
        "emoji": "🫁",
    },
    {
        "topic": "自己免疫性肝疾患",
        "query": '("Hepatitis, Autoimmune"[MeSH] OR "Cholangitis, Sclerosing"[MeSH] OR "Liver Cirrhosis, Biliary"[MeSH]) AND "humans"[MeSH]',
        "field": "肝臓",
        "emoji": "🫁",
    },
    {
        "topic": "B型・C型肝炎の治療",
        "query": '("Hepatitis B/therapy"[MeSH] OR "Hepatitis C/therapy"[MeSH]) AND "humans"[MeSH]',
        "field": "肝臓",
        "emoji": "🫁",
    },
    # --- 膵胆道 ---
    {
        "topic": "膵癌の早期診断と治療",
        "query": '"Pancreatic Neoplasms/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "膵胆道",
        "emoji": "💛",
    },
    {
        "topic": "急性膵炎の管理",
        "query": '"Pancreatitis/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "膵胆道",
        "emoji": "💛",
    },
    {
        "topic": "胆道疾患の診断と治療",
        "query": '"Biliary Tract Diseases/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "膵胆道",
        "emoji": "💛",
    },
    # --- 消化管腫瘍 ---
    {
        "topic": "大腸癌のスクリーニングと治療",
        "query": '"Colorectal Neoplasms/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "消化管腫瘍",
        "emoji": "🎗️",
    },
    {
        "topic": "胃癌の最新治療",
        "query": '"Stomach Neoplasms/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "消化管腫瘍",
        "emoji": "🎗️",
    },
    {
        "topic": "食道癌の治療戦略",
        "query": '"Esophageal Neoplasms/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "消化管腫瘍",
        "emoji": "🎗️",
    },
    {
        "topic": "消化管腫瘍と免疫療法",
        "query": '("Gastrointestinal Neoplasms"[MeSH]) AND ("Immunotherapy"[MeSH]) AND "humans"[MeSH]',
        "field": "消化管腫瘍",
        "emoji": "🎗️",
    },
    # --- 上部消化管 ---
    {
        "topic": "GERD の病態と治療",
        "query": '"Gastroesophageal Reflux/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "上部消化管",
        "emoji": "🔴",
    },
    {
        "topic": "H. pylori 除菌と胃疾患",
        "query": '"Helicobacter pylori"[MeSH] AND ("Eradication"[Title/Abstract] OR "therapy"[Subheading]) AND "humans"[MeSH]',
        "field": "上部消化管",
        "emoji": "🔴",
    },
    {
        "topic": "好酸球性食道炎",
        "query": '"Eosinophilic Esophagitis"[MeSH] AND "humans"[MeSH]',
        "field": "上部消化管",
        "emoji": "🔴",
    },
    # --- 下部消化管 ---
    {
        "topic": "過敏性腸症候群の治療",
        "query": '"Irritable Bowel Syndrome/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "機能性疾患",
        "emoji": "🧠",
    },
    {
        "topic": "慢性便秘の管理",
        "query": '"Constipation/therapy"[MeSH] AND "humans"[MeSH]',
        "field": "機能性疾患",
        "emoji": "🧠",
    },
    {
        "topic": "セリアック病の最新知見",
        "query": '"Celiac Disease"[MeSH] AND "humans"[MeSH]',
        "field": "下部消化管",
        "emoji": "🔵",
    },
    # --- 腸内細菌叢 ---
    {
        "topic": "腸内細菌叢と消化器疾患",
        "query": '"Gastrointestinal Microbiome"[MeSH] AND ("Gastrointestinal Diseases"[MeSH]) AND "humans"[MeSH]',
        "field": "腸内細菌叢",
        "emoji": "🦠",
    },
    {
        "topic": "FMT (糞便微生物移植) の臨床応用",
        "query": '"Fecal Microbiota Transplantation"[MeSH] AND "humans"[MeSH]',
        "field": "腸内細菌叢",
        "emoji": "🦠",
    },
    # --- 内視鏡 ---
    {
        "topic": "内視鏡的治療の進歩",
        "query": '("Endoscopic Mucosal Resection"[MeSH] OR "Endoscopic Submucosal Dissection"[Title]) AND "humans"[MeSH]',
        "field": "内視鏡",
        "emoji": "🔬",
    },
    {
        "topic": "AIと消化器内視鏡",
        "query": '("Artificial Intelligence"[MeSH] OR "Deep Learning"[MeSH]) AND ("Endoscopy, Gastrointestinal"[MeSH]) AND "humans"[MeSH]',
        "field": "内視鏡",
        "emoji": "🔬",
    },
    {
        "topic": "カプセル内視鏡の進展",
        "query": '"Capsule Endoscopy"[MeSH] AND "humans"[MeSH]',
        "field": "内視鏡",
        "emoji": "🔬",
    },
    # --- 栄養・その他 ---
    {
        "topic": "消化器疾患と栄養療法",
        "query": '("Gastrointestinal Diseases/diet therapy"[MeSH] OR "Nutritional Support"[MeSH]) AND ("Gastrointestinal Diseases"[MeSH]) AND "humans"[MeSH]',
        "field": "栄養",
        "emoji": "🥗",
    },
    {
        "topic": "消化器疾患と肥満",
        "query": '("Obesity"[MeSH]) AND ("Gastrointestinal Diseases"[MeSH]) AND "humans"[MeSH]',
        "field": "その他",
        "emoji": "📋",
    },
]

# ============================================================
# PubMed E-utilities
# ============================================================
ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def search_pubmed(query: str, reldate: int) -> list[str]:
    params = {
        "db": "pubmed",
        "term": query,
        "retmax": MAX_RESULTS,
        "datetype": "edat",
        "reldate": reldate,
        "retmode": "json",
        "sort": "relevance",
    }
    resp = requests.get(ESEARCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("esearchresult", {}).get("idlist", [])


def fetch_articles(pmids: list[str]) -> list[dict]:
    if not pmids:
        return []

    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
        "rettype": "abstract",
    }
    resp = requests.get(EFETCH_URL, params=params, timeout=30)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    articles = []

    for article_elem in root.findall(".//PubmedArticle"):
        pmid = _text(article_elem, ".//PMID")
        title = _full_text(article_elem, ".//ArticleTitle")

        # Abstract
        abstract_parts = []
        for at in article_elem.findall(".//AbstractText"):
            label = at.get("Label", "")
            text = "".join(at.itertext()).strip()
            if label:
                abstract_parts.append(f"[{label}] {text}")
            else:
                abstract_parts.append(text)
        abstract = "\n".join(abstract_parts)

        if not abstract:
            abstract_node = article_elem.find(".//Abstract")
            if abstract_node is not None:
                abstract = "".join(abstract_node.itertext()).strip()

        if not abstract:
            continue

        journal = _full_text(article_elem, ".//Journal/Title")

        # 著者（全員取得、表示は先頭3名）
        all_authors = []
        for author in article_elem.findall(".//Author"):
            last = _text(author, "LastName")
            fore = _text(author, "ForeName")
            if last:
                all_authors.append(f"{last} {fore}".strip())

        display_authors = all_authors[:3]
        if len(all_authors) > 3:
            display_authors.append("et al.")

        first_author = all_authors[0] if all_authors else "Unknown"

        doi = ""
        for aid in article_elem.findall(".//ArticleId"):
            if aid.get("IdType") == "doi":
                doi = aid.text or ""

        # 出版年
        pub_year = _text(article_elem, ".//PubDate/Year")
        if not pub_year:
            medline_date = _text(article_elem, ".//PubDate/MedlineDate")
            if medline_date:
                pub_year = medline_date[:4]

        articles.append({
            "pmid": pmid,
            "title": title,
            "abstract": abstract,
            "journal": journal,
            "authors": ", ".join(display_authors),
            "first_author": first_author,
            "doi": doi,
            "year": pub_year or "2025",
        })

    return articles


def _text(elem, path: str) -> str:
    node = elem.find(path)
    if node is not None and node.text:
        return node.text.strip()
    return ""


def _full_text(elem, path: str) -> str:
    node = elem.find(path)
    if node is not None:
        return "".join(node.itertext()).strip()
    return ""


# ============================================================
# Gemini 2.5 Flash で知見投稿を生成
# ============================================================
def generate_knowledge_post(topic_info: dict, articles: list[dict]) -> dict:
    """
    複数の論文を根拠として、トピックに関する最新知見を
    文献付きで解説する投稿を生成する
    """
    model = genai.GenerativeModel(GEMINI_MODEL)

    # 論文情報をフォーマット
    papers_text = ""
    for i, art in enumerate(articles, 1):
        papers_text += f"""
--- 論文{i} ---
PMID: {art['pmid']}
タイトル: {art['title']}
ジャーナル: {art['journal']} ({art['year']})
著者: {art['authors']}
Abstract:
{art['abstract'][:1500]}
"""

    prompt = f"""あなたは消化器内科の専門医向けに最新の医学知見を紹介する医学教育者です。
以下のトピックについて、提供された論文を根拠として最新の知見を解説してください。

## トピック
{topic_info['topic']}（分野: {topic_info['field']}）

## 提供された論文
{papers_text}

## 出力フォーマット（厳守）

TITLE: （投稿タイトル。トピックの核心を捉えた日本語の1行。）

BODY: （本文。400〜600字程度。以下の要素を含める:
- トピックの臨床的背景を1〜2文で簡潔に述べる
- 提供された論文の知見を統合して解説する（個別の論文紹介ではなく、知見の流れとして記述する）
- 文中で根拠となる論文を引用する際は [1], [2], [3] の形式で番号を振る
- 臨床的な意義や今後の展望を述べる
- 専門医が読んで新しい気づきを得られる内容にする）

REFS: （引用文献リスト。以下の形式で論文番号と対応させる:
[1] FirstAuthor, et al. Journal. Year. PMID: XXXXX
[2] FirstAuthor, et al. Journal. Year. PMID: XXXXX
[3] FirstAuthor, et al. Journal. Year. PMID: XXXXX）

重要: 
- 提供された論文の内容のみに基づいて記述すること
- 提供されていない論文を捏造して引用しないこと
- 全ての主要な主張に [番号] で引用をつけること
"""

    response = model.generate_content(prompt)
    text = response.text

    # パース
    title = ""
    body = ""
    refs = ""

    lines = text.split("\n")
    current_section = None
    section_lines = {"BODY": [], "REFS": []}

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("TITLE:"):
            title = stripped.replace("TITLE:", "").strip()
            current_section = None
        elif stripped.startswith("BODY:"):
            content = stripped.replace("BODY:", "").strip()
            if content:
                section_lines["BODY"].append(content)
            current_section = "BODY"
        elif stripped.startswith("REFS:"):
            content = stripped.replace("REFS:", "").strip()
            if content:
                section_lines["REFS"].append(content)
            current_section = "REFS"
        elif current_section and stripped:
            section_lines[current_section].append(stripped)

    body = "\n".join(section_lines["BODY"]).strip()
    refs = "\n".join(section_lines["REFS"]).strip()

    # 引用文献にPubMedリンクを追加
    refs_with_links = []
    for ref_line in refs.split("\n"):
        ref_line = ref_line.strip()
        if ref_line:
            # PMIDを検出してリンクを追加
            for art in articles:
                if art["pmid"] in ref_line and "https://" not in ref_line:
                    ref_line += f"\n  → https://pubmed.ncbi.nlm.nih.gov/{art['pmid']}/"
                    break
            refs_with_links.append(ref_line)
    refs = "\n".join(refs_with_links)

    return {
        "title": title or topic_info["topic"],
        "body": body or "（投稿生成に失敗しました）",
        "refs": refs,
    }


# ============================================================
# Discord 通知
# ============================================================
FIELD_COLORS = {
    "IBD": 0xE74C3C,
    "肝臓": 0xE67E22,
    "膵胆道": 0xF1C40F,
    "消化管腫瘍": 0x9B59B6,
    "上部消化管": 0xE91E63,
    "下部消化管": 0x3498DB,
    "機能性疾患": 0x00BCD4,
    "腸内細菌叢": 0x4CAF50,
    "内視鏡": 0x2196F3,
    "栄養": 0x8BC34A,
    "その他": 0x95A5A6,
}


def send_discord_post(topic_info: dict, post: dict, articles: list[dict]):
    color = FIELD_COLORS.get(topic_info["field"], 0x95A5A6)

    # 本文（Discordの description は2048文字まで）
    description = post["body"][:2048]

    fields = []

    # 引用文献
    if post["refs"]:
        fields.append({
            "name": "📚 引用文献",
            "value": post["refs"][:1024],
            "inline": False,
        })

    # PubMed 直リンク一覧
    link_lines = []
    for i, art in enumerate(articles, 1):
        link_lines.append(
            f"[{i}] [{art['journal']} ({art['year']})]"
            f"(https://pubmed.ncbi.nlm.nih.gov/{art['pmid']}/)"
        )
    if link_lines:
        fields.append({
            "name": "🔗 論文リンク",
            "value": "\n".join(link_lines)[:1024],
            "inline": False,
        })

    embed = {
        "title": f"{topic_info['emoji']} {post['title']}"[:256],
        "description": description,
        "color": color,
        "fields": fields,
        "footer": {
            "text": f"{topic_info['field']}  |  {datetime.now().strftime('%Y-%m-%d %H:%M')} JST",
        },
        "timestamp": datetime.utcnow().isoformat(),
    }

    payload = {
        "username": "GI Knowledge Bot",
        "embeds": [embed],
    }

    resp = requests.post(GI_KNOWLEDGE_WEBHOOK_URL, json=payload, timeout=15)
    resp.raise_for_status()
    print(f"[Discord] 投稿完了: {post['title'][:50]}")


# ============================================================
# 投稿済み管理（トピック＋PMIDの重複排除）
# ============================================================
def load_posted() -> dict:
    if POSTED_FILE.exists():
        return json.loads(POSTED_FILE.read_text())
    return {"pmids": [], "recent_topics": []}


def save_posted(data: dict):
    # PMIDは直近5000件、トピックは直近100件を保持
    data["pmids"] = data["pmids"][-5000:]
    data["recent_topics"] = data["recent_topics"][-100:]
    POSTED_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))


# ============================================================
# メイン処理
# ============================================================
def main():
    print(f"=== GI Knowledge Bot 実行: {datetime.now().isoformat()} ===")

    posted = load_posted()
    posted_pmids = set(posted["pmids"])
    recent_topics = posted["recent_topics"]

    # 最近使ったトピックを避けてランダム選択
    available = [
        t for t in TOPIC_CATEGORIES
        if t["topic"] not in recent_topics[-15:]  # 直近15投稿と重複しない
    ]
    if not available:
        available = TOPIC_CATEGORIES  # 全部使い切ったらリセット

    topic = random.choice(available)
    print(f"[Topic] {topic['emoji']} {topic['topic']} ({topic['field']})")

    # PubMed検索
    pmids = search_pubmed(topic["query"], reldate=SEARCH_DAYS)
    new_pmids = [p for p in pmids if p not in posted_pmids]
    print(f"[PubMed] 候補 {len(new_pmids)} 件 (全 {len(pmids)} 件中)")

    if len(new_pmids) < PAPERS_PER_POST:
        # 新規が少なければ既出も含めて使う
        new_pmids = pmids[:MAX_RESULTS]
        print(f"[Fallback] 既出含めて {len(new_pmids)} 件使用")

    # ランダムに論文を選んでabstract取得
    selected = random.sample(new_pmids, min(PAPERS_PER_POST + 3, len(new_pmids)))
    articles = fetch_articles(selected)

    if len(articles) < 2:
        print("[Error] abstract付き論文が不足。終了。")
        return

    # 使用する論文を確定
    use_articles = articles[:PAPERS_PER_POST]
    print(f"[Selected] {len(use_articles)} 件の論文を使用")
    for art in use_articles:
        print(f"  - PMID {art['pmid']}: {art['title'][:60]}")

    # 知見投稿を生成
    try:
        post = generate_knowledge_post(topic, use_articles)
        send_discord_post(topic, post, use_articles)

        # 投稿済み記録を更新
        for art in use_articles:
            posted["pmids"].append(art["pmid"])
        posted["recent_topics"].append(topic["topic"])
        save_posted(posted)

        print(f"=== 完了 ===")
    except Exception as e:
        print(f"[Error] {e}")
        raise


if __name__ == "__main__":
    main()
