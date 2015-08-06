#!/usr/bin/env python
# -*- coding: utf-8 -*-

import py2neo
from py2neo.cypher import CreateNode, MergeNode
py2neo.packages.httpstream.http.socket_timeout = 600

nodesdone = {}
class NeoBatch(object):

    def __init__(self, graph, processBatch=150, maxTransactions=10000):
        self.limit = processBatch
        self.max = maxTransactions
        self.nodesdone = {}
        self.init()

    def init(self):
        self.tx = graph.cypher.begin()
        self.todo = 0
        self.done = 0

    def process(self):
        res = self.tx.process()
        self.done += len(res)
        self.todo = 0
        return res

    def commit(self):
        res = self.tx.commit()
        self.done += len(res)
        print "Commit", self.done
        return res

    def reset(self):
        self.commit()
        self.init()

    def append(self, transaction, *args, **kwargs):
        self.todo += 1
        self.tx.append(transaction, *args, **kwargs)
        if self.done + self.todo > self.max:
            self.reset()
        if self.todo == self.limit:
            #self.process()
            self.reset()

def add_stem(tx, lru, page=False):
    # Check cache
    if lru in nodesdone:
        if page and not nodesdone[lru]:
            tx.append(MergeNode("Stem", "lru", lru).set(page=True))
            nodesdone[lru] = True
        return

    # Check graph
    exists = graph.cypher.execute_one("MATCH (p:Stem {lru: {L}}) RETURN p", {"L": lru})
    if exists:
        if page and not exists["page"]:
            tx.append(MergeNode("Stem", "lru", lru).set(page=True))
        nodesdone[lru] = page or exists["page"]
        return

    # Add stem node
    stems = [s for s in lru.split('|') if s]
    stem = stems.pop()
    tx.append(CreateNode("Stem", "lru", lru).set(label=(stem[2:] if len(stem) > 2 else ""), stem=stem[0], page=page))
    nodesdone[lru] = page

    # Add parent stem nodes and heritage links
    if len(stems):
        parent = "|".join(stems) + "|"
        add_stem(tx, parent)
        tx.append("MATCH (p:Stem {lru:{P}}), (c:Stem {lru: {C}}) CREATE UNIQUE (p)-[:H]->(c)", {"P": parent, "C": lru})

def load_page_with_links(tx, pagelru, lrulinks):
    add_stem(tx, pagelru, True)
    for lru in lrulinks:
        add_stem(tx, lru, True)
        tx.append("MATCH (p:Stem {lru:{P}}), (l:Stem {lru:{C}}) CREATE UNIQUE (p)-[:L]->(l)", {"P": pagelru, "C": lru})

def load_webentity(tx, name, status, prefixes):
    tx.append(MergeNode("WebEntity", "name", name).set(status=status))
    for prefix in prefixes:
        add_stem(tx, prefix)
        tx.append("MATCH (w:WebEntity {name:{W}}), (p:Stem {lru:{L}}) CREATE UNIQUE (w)-[:P]->(p)", {"W": name, "L": prefix})

if __name__ == "__main__":
    hyphe_urlapi = "http://localhost/hyphe-api/"
    hyphe = "hyphe-multi"
    corpus = "s"

    # Neo4J Connection
    py2neo.authenticate("localhost:7474", "neo4j", "neo4j")
    graph = py2neo.Graph("http://localhost:7474/db/data/")

    # ResetDB
    #graph.delete_all()
    #graph.schema.create_uniqueness_constraint('Stem', 'lru')

    tx = NeoBatch(graph)

    # Collect WebEntities from Hyphe
    import jsonrpclib
    try:
        hyphe_core = jsonrpclib.Server(hyphe_urlapi, version=1)
        assert(hyphe_core.start_corpus(corpus)['code'] == 'success')
    except:
        logging.error('Could not initiate connection to hyphe core')
        exit(1)
    for WE in hyphe_core.store.get_webentities([], ['status', 'name'], -1, 0, True, False, False, corpus)["result"]:
        print WE["status"], WE["name"]
        load_webentity(tx, WE["name"], WE["status"], WE["lru_prefixes"])
    tx.reset()

    # Collect pages from Mongo
    from pymongo import MongoClient
    pages = MongoClient()[hyphe]["%s.pages" % corpus]
    for page in pages.find({"status": 200}, fields=["lru", "lrulinks"]):
        print page["lru"], len(page["lrulinks"])
        load_page_with_links(tx, page["lru"], page["lrulinks"])
    tx.commit()


