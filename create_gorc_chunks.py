from typing import Dict, Any, TextIO


import json
import os
import gzip
import argparse

import pysbd

segmenter = pysbd.Segmenter(language="en", clean=False, char_span=True)


def paper_iterator(directory: str, shard: int):

    metadata_path = os.path.join(directory, "metadata", f"metadata_{shard}.jsonl")
    paper_path = os.path.join(directory, "pdf_parses", f"pdf_parses_{shard}.jsonl")

    with open(metadata_path, "r") as metadata_f, open(paper_path, "rt") as f:
        for paper, meta in zip(f, metadata_f):

            yield json.loads(paper), json.loads(meta)


def linked_refs_in_chunk(cite_spans, refs_with_links):

    return [cite for cite in cite_spans if cite["ref_id"] in refs_with_links]


def create_abstract(paper_blob, metadata, out):

    # We only need to record abstracts with incoming citation edges.
    if not metadata["has_inbound_citations"]:
        return

    if not paper_blob["abstract"] and metadata["abstract"] is None:
        return

    if metadata["abstract"]:
        abstract = metadata["abstract"]
    else:
        abstract = paper_blob["abstract"][0]["text"]

    blob = {
        "paper_id": paper_blob["paper_id"],
        "title": metadata["title"],
        "abstract": abstract
    }
    out.write(json.dumps(blob) + "\n")


def parse_paper_abstract(blob):
    chunks = []
    for chunk in blob:
        chunks.append({
            "text": chunk["text"],
            "paper_ids": [],
        })
    return chunks

def parse_paper_part(blob, refs_with_links):

    chunks = []

    for chunk in blob:
        linked_refs = linked_refs_in_chunk(chunk["cite_spans"], refs_with_links)
        if not linked_refs:
            continue

        try:
            # Pysbd is not the most robust piece of software.
            sentences = segmenter.segment(chunk["text"])
        except:
            print("pysbd_error!")
            continue

        num_sents = len(sentences)
        i = 0
        linked_refs = list(reversed(linked_refs))
        while i < num_sents and linked_refs:
            end = sentences[i].end
            paper_ids_for_window = []
            while linked_refs:
                cite_span = linked_refs.pop()
                if end > cite_span["end"]:
                    paper_ids_for_window.append(refs_with_links[cite_span["ref_id"]])
                else:
                    # outside window context, put it back
                    linked_refs.append(cite_span)
                    break

            # Only add chunk if we found refs
            if paper_ids_for_window:
                start = max(0, i - 2)
                context = " ".join([x.sent for x in sentences[start: start + 4]])
                chunks.append({
                    "text": context,
                    "paper_ids": paper_ids_for_window,
                    "section": chunk["section"]
                    })

            i += 1

    return chunks

def create_chunks(
    paper_blob: Any,
    metadata: Dict[str, str],
    out: TextIO = None,
    filter_by: str = None,
    abstract_only: bool = False):

    if not metadata["has_pdf_parse"]:
        return

    if filter_by is not None:
        if filter_by == "pubmed" and metadata["pmid"] is None:
            return
        if filter_by == "acl" and metadata["acl_id"] is None:
            return
        if filter_by == "arxiv" and metadata["arxiv_id"] is None:
            return
        raise ValueError("Unknown filter string. Valid values are [arxiv, pubmed, acl]")


    paper_id = paper_blob["paper_id"]
    paper_title = metadata["title"]

    refs_with_links = {
            ref: content["link"]
            for ref, content in paper_blob["bib_entries"].items()
            if content["link"]
        }

    chunks = []
    if abstract_only:
        chunks.extend(parse_paper_abstract(paper_blob["abstract"]))
    else:
        chunks.extend(parse_paper_part(paper_blob["abstract"], refs_with_links))

        chunks.extend(parse_paper_part(paper_blob["body_text"], refs_with_links))


    if out is not None and chunks:
        blob = {
            "title": paper_title,
            "paper_id": paper_id,
            "chunks": chunks,
            "references": list(refs_with_links.values())
            }
        out.write(json.dumps(blob) + "\n")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('in_dir', type=str, help="Input directory.")
    parser.add_argument('out_dir', type=str, help="Output directory.")
    parser.add_argument('--start', type=int, help="Start shard index", required=True)
    parser.add_argument('--end', type=int, default=None, help="End shard index")
    parser.add_argument('--filter', type=str, default=None, help="Filter to arxiv or pubmed only.")
    parser.add_argument('--abstracts', action="store_true", help="Only extract abstracts")

    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(os.path.join(args.out_dir, "abstracts"), exist_ok=True)
    os.makedirs(os.path.join(args.out_dir, "chunks"), exist_ok=True)

    # If end is not passed, we are only processing a single chunk.
    if args.end is None:
        args.end = args.start + 1

    for i in range(args.start, args.end):
        abstract = os.path.join(args.out_dir, "abstracts", f"{i}.jsonl")
        chunks = os.path.join(args.out_dir, "chunks", f"{i}.jsonl")
        if os.path.exists(abstract) or os.path.exists(chunks):
            print(f"Found existing chunks and abstracts for shard {i}. Skipping.")
            continue
        with open(abstract, "w+") as f, open(chunks, "w+") as f2:

            i = 0
            for (paper, meta) in paper_iterator(args.in_dir, i):
                create_chunks(paper, meta, f2, args.filter, args.abstracts)
                create_abstract(paper, meta, f)
                i+=1
                if i == 200: break
