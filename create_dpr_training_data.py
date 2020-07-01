


import json
import os
import argparse
import random


def load_abstracts(abstract_path: str):

    all_abstracts = {}
    for f in os.listdir(abstract_path):
        with open(os.path.join(abstract_path, f)) as fin:
            
            for line in fin:
                blob = json.loads(line)
                if blob["abstract"] is None:
                    continue
                all_abstracts[blob["paper_id"]] = blob

    return all_abstracts

def sample_training_data(chunk_path: str, out_dir: str, abstracts):

    shard_size = 50000
    data = []
    count = 0
    for f in os.listdir(chunk_path):
        for line in open(os.path.join(chunk_path, f), "r"):

            paper = json.loads(line)
            title = paper["title"]
            references = set(paper["references"])
            paper_id = paper["paper_id"]

            for chunk in paper["chunks"]:
                text = chunk["text"]
                positive_ids = set(chunk["paper_ids"])

                negative_paper_ids = references.difference(positive_ids)
                if not negative_paper_ids:
                    continue

                positive_abstract = None
                while positive_ids:
                    positive = random.sample(positive_ids, 1)[0]
                    positive_abstract = abstracts.get(positive, None)
                    if positive_abstract is not None:
                        break
                    else:
                        positive_ids.remove(positive)

                if positive_abstract is None:
                    continue

                negative_abstract = None
                while negative_paper_ids:
                    negative = random.sample(negative_paper_ids, 1)[0]

                    negative_abstract = abstracts.get(negative, None)
                    if negative_abstract is not None:
                        break
                    else:
                        negative_paper_ids.remove(negative)
                if negative_abstract is None:
                    continue

                blob = {
                    "question": "N/A",
                    "answers": ["N/A"],
                    "source_ctxs": [{
                        "title": title,
                        "text": text,
                    }],
                    "positive_ctxs": [{
                        "title": positive_abstract["title"],
                        "text": positive_abstract["abstract"],
                    }],
                    "negative_ctxs": [],
                    "hard_negative_ctxs": [{
                        "title": negative_abstract["title"],
                        "text": negative_abstract["abstract"],
                    }]
                }
                data.append(blob)
                count += 1

                if count % shard_size == 0 and count != 0:
                    with open(os.path.join(out_dir, f"{count}.json"), "w+") as out:
                        json.dump(data, out)
                    
                    data = []

    if data:
        with open(os.path.join(out_dir, f"{count}.json"), "w+") as out:
            json.dump(data, out)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('in_dir', type=str, help="Input directory.")
    parser.add_argument('out_dir', type=str, help="Output directory.")

    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    abstracts = load_abstracts(os.path.join(args.in_dir, "abstracts"))

    sample_training_data(os.path.join(args.in_dir, "chunks"), args.out_dir, abstracts)
