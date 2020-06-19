


import json
import os
import gzip
import argparse

import pysbd

segmenter = pysbd.Segmenter(language="en", clean=False, char_span=True)


def paper_iterator(directory: str, shard: int):

    metadata_path = os.path.join(directory, "metadata", f"{shard}.tsv")
    paper_path = os.path.join(directory, "papers", f"{shard}.jsonl.gz")

    with open(metadata_path, "r") as metadata_f, gzip.open(paper_path, "rt") as f:
        first = metadata_f.readline().strip("\n").split("\t")
        for line in f:
            values = metadata_f.readline().strip("\n").split("\t")
            metadata = {k: v if v != "" else None for k, v in zip(first, values)}
            yield json.loads(line), metadata


def linked_refs_in_chunk(cite_spans, refs_with_links):

    return [cite for cite in cite_spans if cite["ref_id"] in refs_with_links]


def create_abstract(paper_blob, metadata, out):

    # We only need to record abstracts with incoming citation edges.
    if metadata["inbound_citations"] == "[]":
        return
    if metadata["has_grobid"] == "False":
        return
    if metadata["has_grobid_text"] == "False":
        return

    if not paper_blob["grobid_parse"]["abstract"] and not metadata["has_gold_abs"] == "True":
        return

    if metadata["has_gold_abs"]:
        abstract = paper_blob["metadata"]["abstract"]
    else:
        abstract = paper_blob["grobid_parse"]["abstract"][0]["text"]

    blob = {
        "paper_id": paper_blob["paper_id"],
        "title": paper_blob["metadata"]["title"],
        "abstract": abstract
    }
    out.write(json.dumps(blob) + "\n")


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
                    })

            i += 1

    return chunks

def create_chunks(paper_blob, metadata, out = None):
    if metadata["has_grobid"] == "False":
        return
    if metadata["has_grobid_text"] == "False":
        return
    if int(metadata["grobid_num_linked_bibs"] or 0) < 4:
        return

    paper_id = paper_blob["paper_id"]
    paper_title = paper_blob["metadata"]["title"]
    grobid_parse = paper_blob["grobid_parse"]

    refs_with_links = {
            ref: content["links"]
            for ref, content in grobid_parse["bib_entries"].items()
            if content["links"]
        }

    chunks = []
    chunks.extend(parse_paper_part(grobid_parse["abstract"], refs_with_links))
    chunks.extend(parse_paper_part(grobid_parse["body_text"], refs_with_links))

    if out is not None:
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
    parser.add_argument('--start', type=int, help="Start shard index")
    parser.add_argument('--end', type=int, help="End shard index")

    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(os.path.join(args.out_dir, "abstracts"), exist_ok=True)
    os.makedirs(os.path.join(args.out_dir, "chunks"), exist_ok=True)

    for i in range(args.start, args.end):
        abstract = os.path.join(args.out_dir, "abstracts", f"{i}.jsonl")
        chunks = os.path.join(args.out_dir, "chunks", f"{i}.jsonl")
        if os.path.exists(abstract) or os.path.exists(chunks):
            print(f"Found existing chunks and abstracts for shard {i}. Skipping.")
            continue
        with open(abstract, "w+") as f, open(chunks, "w+") as f2:
            for (paper, meta) in paper_iterator(args.in_dir, i):
                create_chunks(paper, meta, f2)
                create_abstract(paper, meta, f)
