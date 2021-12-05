import json
import time
import re
import argparse
import sys
import requests

from wikidata_linker_utils.wikipedia import iterate_articles

from multiprocessing import Pool


redirection_link_pattern = re.compile(r"(?:#REDIRECT|#weiterleitung|#REDIRECCIÓN|REDIRECIONAMENTO)\s*\[\[([^\]\[]*)\]\]", re.IGNORECASE)
anchor_link_pattern = re.compile(r"\[\[([^\]\[:]*)\]\]")


def redirection_link_job(args):
    """
    Performing map-processing on different articles
    (in this case, just remove internal links)
    """
    article_name, lines = args
    found_tags = []
    for match in re.finditer(redirection_link_pattern, lines):
        if match is None:
            continue
        if match.group(1) is None:
            continue
        match_string = match.group(1).strip()
        if "|" in match_string:
            link, _ = match_string.rsplit("|", 1)
            link = link.strip().split("#")[0]
        else:
            link = match_string

        if len(link) > 0:
            found_tags.append(link)
    return (article_name, found_tags)

def mention_finding_job(args):
    article_name, lines = args
    
    article_link = "_".join(article_name.split())
    article_wikidata = requests.get(f"https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&titles={article_link}&format=json").json()
    article_qid = 'Q'+list(article_wikidata['query']['pages'])[0]
    
    mention_num = 0
    mentions = {}
    wikipedia_url_head = "en.wikipedia.org/wiki/"
    
    for match in re.finditer(anchor_link_pattern, lines):
        found_mention = {}
        match_string = match.group(1).strip()
        match_start = match.start()
        match_end = match.end()
        if "|" in match_string:
            link, anchor = match_string.rsplit("|", 1)
            link = link.strip().split("#")[0]
            anchor = anchor.strip()
        else:
            anchor = match_string
            link = match_string
        
        # Anchor->Mention, Link->Entity
        entity_link = "_".join(link.split())
        wikipedia_url = wikipedia_url_head + entity_link
        wikidata_json = requests.get(f"https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&titles={entity_link}&format=json").json()
        qid, wikipedia_id, entity_desc = "", "", ""
        
        try:
            qid = list(wikidata_json['query']['pages'])[0]
            wikipedia_id = wikidata_json['query']['pages'][qid]['pageprops']['wikibase_item']
            entity_desc = wikidata_json['query']['pages'][qid]['pageprops']['wikibase-shortdesc']
        except KeyError:
            continue
        
        # Get Context
        tot_context = 512
        if len(lines) - len(match_string) > 512:
            if match_start-256 >= 0:
                left_context = lines[match_start-256:match_start]
            else:
                left_context = lines[:match_start]
            if match_end+256 > len(lines):
                right_context = lines[match_end:]
            else:
                right_context = lines[match_end:match_end+256]
        else:
            left_context = lines[:match_start]
            right_context = lines[match_end:]
        
        # Make final mentions
        if len(anchor) > 0 and len(link) > 0:
            mention_num += 1
            found_mention["article_id"] = article_qid
            found_mention["mention"] = anchor
            found_mention["left_context"] = left_context
            found_mention["right_context"] = right_context
            found_mention["wikipedia_title"] = link
            found_mention["wikipedia_id"] = wikipedia_id
            found_mention["wikipedia_url"] = wikipedia_url
            found_mention["entity_desc"] = entity_desc
            mentions[f"{article_name}_{mention_num}"] = found_mention
    return (article_name, mentions)

def anchor_finding_job(args):
    """
    Performing map-processing on different articles
    (in this case, just remove internal links)
    """
    article_name, lines = args
    found_tags = []
    for match in re.finditer(anchor_link_pattern, lines):
        match_string = match.group(1).strip()
        
        if "|" in match_string:
            link, anchor = match_string.rsplit("|", 1)
            link = link.strip().split("#")[0]
            anchor = anchor.strip()
        else:
            anchor = match_string
            link = match_string

        if len(anchor) > 0 and len(link) > 0:
            found_tags.append((anchor, link))
            
    return (article_name, found_tags)



def anchor_category_redirection_link_job(args):
    article_name, found_redirections = redirection_link_job(args)
    article_name, found_anchors = anchor_finding_job(args)
    article_name, found_mentions = mention_finding_job(args)
    #= mention_finding_job(args)
    return (article_name, (found_anchors, found_redirections, found_mentions))


def run_jobs(worker_pool, pool_jobs, outfile_anchors, outfile_redirections, outfile_mentions):
    final_mentions = {}
    results = worker_pool.map(anchor_category_redirection_link_job, pool_jobs)
    for article_name, result in results:
        anchor_links, redirect_links, mentions = result
        for link in redirect_links:
            outfile_redirections.write(article_name + "\t" + link + "\n")
        if ":" not in article_name:
            outfile_anchors.write(article_name + "\t" + article_name + "\t" + article_name + "\n")
            for anchor, link in anchor_links:
                outfile_anchors.write(article_name + "\t" + anchor + "\t" + link + "\n")
            final_mentions.update(mentions)
    json.dump(final_mentions, outfile_mentions)


def parse_wiki(path,
               anchors_path,
               redirections_path,
               mentions_path,
               threads=1,
               max_jobs=10):
    '''
    Run jobs to parse wikipedia
    '''
    t0 = time.time()
    jobs = []
    pool = Pool(processes=threads)
    try:
        with open(redirections_path, "wt") as fout_redirections, open(anchors_path, "wt") as fout_anchors, open(mentions_path, "wt") as fout_mentions:
            # Go through each article and find mentions, anchors, redirections
            for article_name, lines in iterate_articles(path):
                jobs.append((article_name, lines))
                if len(jobs) >= max_jobs:
                    run_jobs(pool, jobs, fout_anchors, fout_redirections, fout_mentions)
                    jobs = []
            if len(jobs) > 0:
                run_jobs(pool, jobs, fout_anchors, fout_redirections, fout_mentions)
                jobs = []
    finally:
        pool.close()
    t1 = time.time()
    print("%.3fs elapsed." % (t1 - t0,))


def parse_args(argv=None):
    '''
    parse command line arguments
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("wiki",
        help="Wikipedia dump file (xml).")
    parser.add_argument("out_anchors",
        help="File where anchor information should be saved (tsv).")
    parser.add_argument("out_redirections",
        help="File where redirection information should be saved (tsv).")
    parser.add_argument("out_mentions",
        help="Folder where mention files should be saved (JSON).")

    def add_int_arg(name, default):
        parser.add_argument("--%s" % (name,), type=int, default=default)

    add_int_arg("threads", 8)
    add_int_arg("max_jobs", 10000)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    parse_wiki(
        path=args.wiki,
        anchors_path=args.out_anchors,
        redirections_path=args.out_redirections,
        mentions_path=args.out_mentions,
        threads=args.threads,
        max_jobs=args.max_jobs
    )

if __name__ == "__main__":
    main()

