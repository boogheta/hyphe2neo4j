#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import time
import py2neo
from py2neo.cypher import CreateNode, MergeNode
py2neo.packages.httpstream.http.socket_timeout = 600

class NeoBatch(object):

    def __init__(self, graph, processBatch=200, cacheNodes={}):
        self.limit = processBatch
        self.nodesdone = cacheNodes
        self.init()

    def init(self):
        self.tx = graph.cypher.begin()
        self.todo = 0

    def commit(self):
        t = time()
        res = self.tx.commit()
        t2 = time() - t
        rate = len(res)/t2
        print "Commit", len(res), "in", t2, "s ->", "%s/s" % rate
        return res

    def reset(self):
        self.commit()
        self.init()

    def append(self, transaction, *args, **kwargs):
        self.todo += 1
        self.tx.append(transaction, *args, **kwargs)
        if self.todo == self.limit:
            self.reset()

def add_stem(tx, lru, page=False, checkGraph=False):
    # Check cache
    if lru in tx.nodesdone:
        if page and not tx.nodesdone[lru]:
            tx.append(MergeNode("Stem", "lru", lru).set(page=True))
            tx.nodesdone[lru] = True
        return

    ## Check graph
    if checkGraph:
        exists = graph.cypher.execute_one("MATCH (p:Stem {lru: {L}}) RETURN p", {"L": lru})
        if exists:
            if page and not exists["page"]:
                tx.append(MergeNode("Stem", "lru", lru).set(page=True))
            tx.nodesdone[lru] = page or exists["page"]
            return

    # Add stem node
    stems = [s for s in lru.split('|') if s]
    stem = stems.pop()
    tx.append(CreateNode("Stem", lru=lru, name=(stem[2:] if len(stem) > 2 else ""), stem=stem[0], page=page))
    tx.nodesdone[lru] = page

    # Add parent stem nodes and heritage links
    if len(stems):
        parent = "|".join(stems) + "|"
        add_stem(tx, parent)
        tx.append("MATCH (p:Stem {lru:{P}}), (c:Stem {lru: {C}}) CREATE UNIQUE (p)-[:HERIT]->(c)", {"P": parent, "C": lru})

def load_page_with_links(tx, pagelru, lrulinks):
    add_stem(tx, pagelru, True)
    for lru in lrulinks:
        add_stem(tx, lru, True)
        tx.append("MATCH (p:Stem {lru:{P}}), (l:Stem {lru:{L}}) CREATE UNIQUE (p)-[:LINK]->(l)", {"P": pagelru, "L": lru})

def load_webentity(tx, weid, name, status, prefixes):
    tx.append(CreateNode("WebEntity", id=weid, name=name, status=status))
    for prefix in prefixes:
        add_stem(tx, prefix)
        tx.append("MATCH (w:WebEntity {id:{W}}), (p:Stem {lru:{P}}) CREATE UNIQUE (w)-[:PREFIX]->(p)", {"W": weid, "P": prefix})

if __name__ == "__main__":
    hyphe_urlapi = "http://localhost/hyphe-api/"
    hyphe = "hyphe-multi"
    corpus = "s"

    # Neo4J Connection
    py2neo.authenticate("localhost:7474", "neo4j", "neo4j")
    graph = py2neo.Graph("http://localhost:7474/db/data/")

    # Prepare filesystem
    import os, sys, json
    pagesfile = os.path.join(".cache", "pages.done")
    wesfile = os.path.join(".cache", "webentities.done")
    nodesfile = os.path.join(".cache", "nodes.done")
    if not os.path.isdir(".cache"):
        os.makedirs(".cache")
        open(pagesfile, "w").close()
        open(wesfile, "w").close()
        open(nodesfile, "w").close()

    # ResetDB
    if len(sys.argv) > 1:
        graph.delete_all()
        #graph.schema.create_uniqueness_constraint('Stem', 'lru')
        pagesdone = []
        wesdone = []
        nodesdone = {}
    # Or reload cache
    else:
        with open(pagesfile, "r") as f:
            pagesdone = json.load(f)
        with open(wesfile, "r") as f:
            wesdone = json.load(f)
        with open(nodesfile, "r") as f:
            nodesdone = json.load(f)

    tx = NeoBatch(graph, cacheNodes=nodesdone)
    # Clean close on Ctrl+C or at finish
    from signal import signal, SIGINT
    def clean_close(*args):
        print "Stopping..."
        tx.commit()
        with open(pagesfile, "w") as f:
            json.dump(pagesdone, f)
        with open(wesfile, "w") as f:
            json.dump(wesdone, f)
        with open(nodesfile, "w") as f:
            json.dump(tx.nodesdone, f)
        sys.exit(0)
    signal(SIGINT, clean_close)

    # Collect WebEntities from Hyphe
    import jsonrpclib
    try:
        hyphe_core = jsonrpclib.Server(hyphe_urlapi, version=1)
        assert(hyphe_core.start_corpus(corpus)['code'] == 'success')
    except:
        logging.error('Could not initiate connection to hyphe core')
        exit(1)
    for WE in hyphe_core.store.get_webentities([], ['status', 'name'], -1, 0, True, False, False, corpus)["result"]:
        if WE["id"] in wesdone:
            continue
        print WE["status"], WE["name"]
        load_webentity(tx, WE["id"], WE["name"], WE["status"], WE["lru_prefixes"])
        wesdone.append(WE["id"])
    tx.reset()

    # Collect pages from Mongo
    from pymongo import MongoClient
    pages = MongoClient()[hyphe]["%s.pages" % corpus]
    for page in pages.find({"status": 200}, fields=["lru", "lrulinks"]):
        if page["lru"] in pagesdone:
            continue
        print page["lru"], len(page["lrulinks"])
        load_page_with_links(tx, page["lru"], page["lrulinks"])
        pagesdones.append(page["lru"])

    clean_close()

